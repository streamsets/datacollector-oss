/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.bigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BigtableTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableTarget.class);
  private static final String GOOGLE_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String REGEX = "[_a-zA-Z0-9][-_.a-zA-Z0-9]*";
  private static final String CONFIG = "CONFIG";
  private static final String COLUMN_FAMILY = "Column Family";
  private static final String FIELD_PATH = "Field Path";
  private static final Pattern fieldNameRegex = Pattern.compile(REGEX);

  private BigtableConfigBean conf;
  private Connection connection;
  private Table table;
  private int rowKeySize = -1;
  private long  timeStamp = 0;

  private Map<String, ColumnAndQualifier> destinationNames = new HashMap<>();
  private ErrorRecordHandler errorRecordHandler;

  private class ColumnAndQualifier {
    byte[] columnFamily;
    byte[] qualifier;

    private ColumnAndQualifier(String columnFamily, String qualifier) {
      this.columnFamily = columnFamily.getBytes();
      this.qualifier = qualifier.getBytes();
    }
  }

  public BigtableTarget(BigtableConfigBean conf) {
    this.conf = conf;
  }

  private void verifyBothParts(BigtableFieldMapping fld, String colFam, String qual, List<ConfigIssue> issues) {
    // column family and qualifier...
    if (fieldNameRegex.matcher(colFam).matches() && fieldNameRegex.matcher(qual).matches()) {
      if (conf.explicitFieldMapping) {
        destinationNames.put(fld.column, new ColumnAndQualifier(colFam, qual));

      } else {
        if (conf.columnFamily.isEmpty()) {
          issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), COLUMN_FAMILY, Errors.BIGTABLE_06, fld.column));
        } else {
          destinationNames.put(fld.column, new ColumnAndQualifier(conf.columnFamily, qual));
        }
      }
    } else {
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), COLUMN_FAMILY, Errors.BIGTABLE_16, qual));
    }
  }

  private void verify1Part(String qual, List<ConfigIssue> issues) {
    // qualifier only...
    if (conf.columnFamily.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), FIELD_PATH, Errors.BIGTABLE_06, qual));
    } else {
      if (fieldNameRegex.matcher(qual).matches()) {
        destinationNames.put(qual, new ColumnAndQualifier(conf.columnFamily, qual));

      } else {
        issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), FIELD_PATH, Errors.BIGTABLE_16, qual));
      }
    }

  }

  private void verifyColumnFamilyAndQualifiers(List<ConfigIssue> issues) {

    // pre-process and verify destination column mappings.
    if (conf.fieldColumnMapping.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), FIELD_PATH, Errors.BIGTABLE_24));
      return;
    }

    for (BigtableFieldMapping f : conf.fieldColumnMapping) {
      String[] parts = f.column.split(":");
      if (parts.length == 2) {
        verifyBothParts(f, parts[0], parts[1], issues);

      } else if (parts.length == 1) {
        verify1Part(parts[0], issues);

      } else {
        issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), FIELD_PATH, Errors.BIGTABLE_16, f.column));
      }
    }

  }

  private Set<String> uniqueColumnFamilies() {
    HashMap<String, String> temp = new HashMap<>();

    if(!conf.columnFamily.isEmpty()) {
      temp.put(conf.columnFamily, conf.columnFamily);
    }

    // find all unique column families.
    for (ColumnAndQualifier p : destinationNames.values()) {
      temp.put(new String(p.columnFamily), new String(p.columnFamily));
    }
    return temp.keySet();

  }

  private Connection connectToBigtable(List<ConfigIssue> issues) {
    Connection conn;
    try {
      conn = BigtableConfiguration.connect(conf.bigtableProjectID, conf.bigtableInstanceID);

    } catch (Exception ex) {
      LOG.error(Errors.BIGTABLE_01.getMessage(), conf.bigtableProjectID, ex.toString(), ex);
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
          CONFIG,
          Errors.BIGTABLE_01,
          conf.bigtableProjectID,
          ex.toString()
      ));
      return null;
    }
    return conn;
  }

  private Admin setUpAdmin(Connection connection, List<ConfigIssue> issues) {
    Admin admin = null;
    try {
      admin = connection.getAdmin();

    } catch (Exception ex) {
      LOG.error(Errors.BIGTABLE_02.getMessage(), ex.toString(), ex);
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
          CONFIG,
          Errors.BIGTABLE_02,
          ex.toString()
      ));
    }
    return admin;
  }

  private Table setUpTable(Connection connection, List<ConfigIssue> issues) {
    Table tab = null;
    try {
      tab = connection.getTable(TableName.valueOf(conf.tableName));

    } catch (Exception ex) {
      LOG.info(Errors.BIGTABLE_19.getMessage(), conf.tableName, ex.toString(), ex);
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
          CONFIG,
          Errors.BIGTABLE_19,
          conf.tableName,
          ex.toString()
      ));
    }
    return tab;
  }

  private void checkEnvironment(List<ConfigIssue> issues) {
    // check if we have an environment variable:
    Map<String, String> vars = System.getenv();
    if (!vars.containsKey(GOOGLE_CREDENTIALS)
        || vars.get(GOOGLE_CREDENTIALS).isEmpty()
        || !Files.exists(Paths.get(vars.get(GOOGLE_CREDENTIALS)
    ))) {
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
          CONFIG,
          Errors.BIGTABLE_07,
          vars.get(GOOGLE_CREDENTIALS),
          GOOGLE_CREDENTIALS
        ));
      }
  }

  private void createTable(Admin admin, List<ConfigIssue> issues) {
    if (conf.createTableAndColumnFamilies) {
      try {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(conf.tableName));
        admin.createTable(descriptor);

      } catch (IOException ex) {
        LOG.info(Errors.BIGTABLE_18.getMessage(), conf.tableName, ex.toString(), ex);
        issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
            CONFIG,
            Errors.BIGTABLE_18,
            conf.tableName,
            ex.toString()
        ));
      }

    } else {
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), CONFIG, Errors.BIGTABLE_22, conf.tableName));
    }

  }

  private void checkRowKeyComponents(List<ConfigIssue> issues) {

    // multi-column row key?
    if (conf.createCompositeRowKey) {
      for (BigtableRowKeyMapping rowKeyMap : conf.rowKeyColumnMapping) {
        if ("".equals(rowKeyMap.rowKeyComponent)) {
          issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
              "Row Key",
              Errors.BIGTABLE_09,
              rowKeyMap.rowKeyComponent
          ));
        }
      }
    } else {
      // single column row key.
      if (conf.singleColumnRowKey == null) {
        issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), "Row Key", Errors.BIGTABLE_11, ""));
      }
    }
  }
  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();

    if(conf.timeBasis == TimeBasis.PIPELINE_START) {
      timeStamp = System.currentTimeMillis();
    }

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    checkEnvironment(issues);

    checkRowKeyComponents(issues);

    verifyColumnFamilyAndQualifiers(issues);


    if(conf.timeBasis == TimeBasis.FROM_RECORD && "".equals(conf.timeStampField)) {
        issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), "Time Basis", Errors.BIGTABLE_03));
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    connection = connectToBigtable(issues);
    if (connection == null || (!issues.isEmpty())) {
      return issues;
    }

    Admin admin = setUpAdmin(connection, issues);
    if (admin == null || (!issues.isEmpty())) {
      return issues;
    }

    try {
      // check if the table already exists:
      if (!admin.isTableAvailable(TableName.valueOf(conf.tableName))) {
        createTable(admin, issues);
      }

    } catch (IOException ex) {
      LOG.info(Errors.BIGTABLE_17.getMessage(), ex.toString(), ex);
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
          "Table Name",
          Errors.BIGTABLE_17,
          ex.toString()
      ));
    }

    table = setUpTable(connection, issues);
    if(!issues.isEmpty()) {
      return issues;
    }

    // check if column families exist...
    // if not, create them if that option is enabled.
    for (String family : uniqueColumnFamilies()) {
      try {
        if (!table.getTableDescriptor().hasFamily(family.getBytes())) {
          if (conf.createTableAndColumnFamilies) {
            createColumnFamily(admin, family, issues);
          } else {
            LOG.info(Errors.BIGTABLE_04.getMessage(), family);
            issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), COLUMN_FAMILY, Errors.BIGTABLE_04, family));
          }
        }
      } catch (IOException ex) {
        LOG.info(Errors.BIGTABLE_05.getMessage(), conf.tableName, ex.toString(), ex);
        issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(), COLUMN_FAMILY, Errors.BIGTABLE_05, ex.toString()));
      }
    }
    return issues;
  }

  private void createColumnFamily(Admin admin, String family, List<ConfigIssue> issues) {
    try {
      admin.addColumn(TableName.valueOf(conf.tableName), new HColumnDescriptor(family));
    } catch (Exception ex) {
      LOG.info(Errors.BIGTABLE_17.getMessage(), ex.toString(), ex);
      issues.add(getContext().createConfigIssue(Groups.BIGTABLE.name(),
          COLUMN_FAMILY,
          Errors.BIGTABLE_17,
          ex.toString()
      ));
    }

  }

  private byte[] buildRowKey(Record rec) throws StageException {

    // simple case - single field row key.
    if (!conf.createCompositeRowKey) {
      Field f = rec.get(conf.singleColumnRowKey);
      if(f == null) {
        errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_11, conf.singleColumnRowKey));
        return new byte [0];
      }

      if (f.getType() == Field.Type.STRING) {
        return f.getValueAsString().getBytes();
      } else {
        return convertToBinary(f, rec);
      }
    }

    // this chunk of initialization code is here
    // instead of in the init routine, because we need
    // build the rowkey from the actual sizes
    // of the component fields - not the values which are
    // declared in the UI.  Well, except for Strings - which
    // are padded (or truncated) to match the width specified
    // in the UI.  For other data types, we use the data type's
    // actual size, when it's converted to a byte array.
    if (rowKeySize == -1) {
      rowKeySize = 0;
      for (BigtableRowKeyMapping component : conf.rowKeyColumnMapping) {
        Field field = rec.get(component.rowKeyComponent);
        if(field == null) {
          errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_10, component.rowKeyComponent));
          return new byte [0];
        }

        if (field.getType() == Field.Type.STRING) {
          rowKeySize += component.columnWidth;
        } else {
          rowKeySize += convertToBinary(field, rec).length;
        }
      }
    }

    // composite key processing...
    byte[] rowKey = new byte[rowKeySize];
    int offset = 0;
    for (BigtableRowKeyMapping component : conf.rowKeyColumnMapping) {
      Field field = rec.get(component.rowKeyComponent);
      if (field == null || "".equals(field.getValueAsString())) {
        errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_10, component.rowKeyComponent));
        return new byte [0];

      } else {
        byte[] ba;
        if (field.getType() == Field.Type.STRING) {
          // if datatype is String, it's likely we'll need to pad or truncate it.
          ba = Arrays.copyOf(field.getValueAsString().getBytes(), component.columnWidth);
          System.arraycopy(ba, 0, rowKey, offset, component.columnWidth);
          offset += component.columnWidth;

        } else {
          // all others, use the field's size.
          ba = convertToBinary(field, rec);
          System.arraycopy(ba, 0, rowKey, offset, ba.length);
          offset += ba.length;
        }
      }
    }
    return rowKey;
  }

  private byte[] convertValue(BigtableFieldMapping map, Field field, Record rec) throws StageException {
    try {
      if (map.storageType.equals(BigtableStorageType.TEXT)) {
        return getBytesForValue(field, rec, map.storageType);

      } else {
        return convertToBinary(field, rec);
      }

    } catch (Exception ex) {
      throw new OnRecordErrorException(rec,
          Errors.BIGTABLE_21,
          map.source,
          field.getType(),
          map.storageType,
          ex);
    }
  }

  @Override
  public void write(final Batch batch) throws StageException {

    long counter = 0;

    if (conf.timeBasis == TimeBasis.BATCH_START) {
      timeStamp = System.currentTimeMillis();
    }

    List<Put> theList = new ArrayList<>();
    Iterator<Record> recordIter = batch.getRecords();
    while (recordIter.hasNext()) {
      Record rec = recordIter.next();

      byte[] rowKey = buildRowKey(rec);
      if (rowKey.length == 0) {
        continue;   // next record.
      }

      if(conf.timeBasis == TimeBasis.SYSTEM_TIME) {
        timeStamp = System.currentTimeMillis();

      } else if(conf.timeBasis == TimeBasis.FROM_RECORD) {
        Field f = rec.get(conf.timeStampField);
        if(f != null) {
          if(f.getType() == Field.Type.LONG) {
              timeStamp = f.getValueAsLong();

          } else {  // the field's data type is wrong.
            errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_08,
                f.getType().name(),
                conf.timeStampField
            ));
            continue;

          }

        } else {  // the field does not exist.
            errorRecordHandler.onError(new OnRecordErrorException(rec,
                Errors.BIGTABLE_14,
                conf.timeStampField
            ));
          continue;  // next record.

        }
      }

      /* SDC-4628.  if "Ignore Missing Data Values" is enabled, we need to determine
      if any columns will be inserted for this record.

      if no data from any column will be inserted, this record probably should to go
      the On Record Error dest, since there will be no trace of this Row Key in
      Bigtable - at least one field has to be inserted so there is a record of
      this Row Key.
       */
      Map<String, byte[]> values = new HashMap<>();

      int nullFields = 0;
      int cantConvert = 0;
      for (BigtableFieldMapping f : conf.fieldColumnMapping) {
        Field tempField = rec.get(f.source);
        if (tempField == null) {
          nullFields++;

        } else {
          // field exists - check if it's convertible.
          try {
            values.put(f.source, convertValue(f, tempField, rec));
          } catch (OnRecordErrorException ex) {
            cantConvert++;
          }
        }
      }

      // any conversion failures go to record error.
      if (cantConvert > 0) {
        errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_23));
        continue;
      }

      if (!conf.ignoreMissingFields) {
        if (nullFields > 0) {   // missing fields, not ignoring them - record goes to error.
          errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_23));
          continue;
        }
      } else {
        // null field count matches field path count.  all columns are null.
        if (nullFields == conf.fieldColumnMapping.size()) {
          errorRecordHandler.onError(new OnRecordErrorException(rec, Errors.BIGTABLE_23));
          continue;   // next record.
        }
      }

      Put put = new Put(rowKey, timeStamp);

      for (BigtableFieldMapping f : conf.fieldColumnMapping) {
        if (values.containsKey(f.source)) {
          theList.add(put.addColumn(destinationNames.get(f.column).columnFamily,
              destinationNames.get(f.column).qualifier,
              timeStamp,
              values.get(f.source)
          ));
        }
      }

      counter++;
      if (counter >= conf.numToBuffer) {
        LOG.debug("Calling put for list of '{}' Records", counter);
        commitRecords(theList);
        theList.clear();
        counter = 0;
      }
    }

    // commit any "leftovers".
    if (counter > 0) {
      commitRecords(theList);
    }
  }

  private void commitRecords(List<Put> theList) throws StageException {
    try {
      table.put(theList);
    } catch (IOException ex) {
      LOG.error(Errors.BIGTABLE_20.getMessage(), ex.toString(), ex);
      throw new StageException(Errors.BIGTABLE_20, ex.toString(), ex);
    }
  }

  @Override
  public void destroy() {
    try {
      if (connection != null) {
        connection.close();
      }

    } catch (IOException ex) {
      LOG.error("destroy(): IOException closing connection: '{}' ", ex.toString(), ex);
    }
  }

  private byte[] getBytesForValue(
      Field field, Record record, BigtableStorageType columnStorageType
  ) throws OnRecordErrorException {

    byte[] value;

    // Figure the storage type and convert appropriately
    if (columnStorageType == (BigtableStorageType.TEXT)) {
      switch (field.getType()) {
        case BYTE_ARRAY:
        case MAP:
        case LIST_MAP:
        case LIST:
          throw new OnRecordErrorException(record,
              Errors.BIGTABLE_12,
              field.getType(),
              BigtableStorageType.TEXT.name()
          );

        default:
          value = Bytes.toBytes(field.getValueAsString());
          break;
      }
    } else {
      value = convertToBinary(field, record);
    }

    return value;
  }

  private byte[] convertToBinary(Field field, Record record) throws OnRecordErrorException {
    byte[] value;
    switch (field.getType()) {
      case BOOLEAN:
        value = Bytes.toBytes(field.getValueAsBoolean());
        break;
      case BYTE:
        value = Bytes.toBytes(field.getValueAsByte());
        break;
      case BYTE_ARRAY:
        value = field.getValueAsByteArray();
        break;
      case CHAR:
        value = Bytes.toBytes(field.getValueAsChar());
        break;
      case DATE:
        throw new OnRecordErrorException(record,
            Errors.BIGTABLE_12,
            Field.Type.DATE.name(),
            BigtableStorageType.BINARY.name()
        );
      case TIME:
        throw new OnRecordErrorException(record,
            Errors.BIGTABLE_12,
            Field.Type.TIME.name(),
            BigtableStorageType.BINARY.name()
        );
      case DATETIME:
        throw new OnRecordErrorException(record,
            Errors.BIGTABLE_12,
            Field.Type.DATETIME.name(),
            BigtableStorageType.BINARY.name()
        );
      case DECIMAL:
        value = Bytes.toBytes(field.getValueAsDecimal());
        break;
      case DOUBLE:
        value = Bytes.toBytes(field.getValueAsDouble());
        break;
      case FLOAT:
        value = Bytes.toBytes(field.getValueAsFloat());
        break;
      case INTEGER:
        value = Bytes.toBytes(field.getValueAsInteger());
        break;
      case LIST:
        throw new OnRecordErrorException(record,
            Errors.BIGTABLE_12,
            Field.Type.LIST.name(),
            BigtableStorageType.BINARY.name()
        );
      case LIST_MAP:
        throw new OnRecordErrorException(record,
            Errors.BIGTABLE_12,
            Field.Type.LIST_MAP.name(),
            BigtableStorageType.BINARY.name()
        );
      case LONG:
        value = Bytes.toBytes(field.getValueAsLong());
        break;
      case MAP:
        throw new OnRecordErrorException(Errors.BIGTABLE_12,
            Field.Type.MAP.name(),
            BigtableStorageType.BINARY.name(),
            record
        );
      case SHORT:
        value = Bytes.toBytes(field.getValueAsShort());
        break;
      case STRING:
        throw new OnRecordErrorException(record,
            Errors.BIGTABLE_12,
            Field.Type.STRING.name(),
            BigtableStorageType.BINARY.name()
        );
      default:
        throw new OnRecordErrorException(record, Errors.BIGTABLE_13, field.toString());
    }
    return value;
  }

}
