/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.hbase.api.impl;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.hbase.api.HBaseConnectionHelper;
import com.streamsets.pipeline.hbase.api.HBaseProducer;
import com.streamsets.pipeline.hbase.api.common.Errors;
import com.streamsets.pipeline.hbase.api.common.FieldConversionException;
import com.streamsets.pipeline.hbase.api.common.producer.ColumnInfo;
import com.streamsets.pipeline.hbase.api.common.producer.Groups;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseColumn;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseConnectionConfig;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractHBaseProducer implements HBaseProducer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseProducer.class);

  protected Stage.Context context;
  protected HBaseConnectionHelper hbaseConnectionHelper;
  protected final HBaseConnectionConfig hBaseConnectionConfig;
  protected ErrorRecordHandler errorRecordHandler;
  protected Date batchTime;

  public AbstractHBaseProducer(
      Stage.Context context,
      AbstractHBaseConnectionHelper hbaseConnectionHelper,
      HBaseConnectionConfig hBaseConnectionConfig,
      ErrorRecordHandler errorRecordHandler
  ) {
    this.context = context;
    this.hbaseConnectionHelper = hbaseConnectionHelper;
    this.hBaseConnectionConfig = hBaseConnectionConfig;
    this.errorRecordHandler = errorRecordHandler;
  }

  public abstract void writeRecordsInHBase(
      Batch batch,
      StorageType rowKeyStorageType,
      String hbaseRowKey,
      String timeDriver,
      Map<String, ColumnInfo> columnMappings,
      boolean ignoreMissingField,
      boolean implicitFieldMapping,
      boolean ignoreInvalidColumn
  ) throws StageException;

  public abstract void destroyTable();

  public abstract void createTable(String tableName) throws InterruptedException, IOException;

  public abstract void checkHBaseAvailable(List<Stage.ConfigIssue> issues);

  protected abstract void addCell(Put p, byte[] columnFamily, byte[] qualifier, Date recordTime, byte[] value);

  public HBaseConnectionHelper getHBaseConnectionHelper() {
    return hbaseConnectionHelper;
  }

  protected Stage.Context getContext() {
    return context;
  }

  private static void validateRootLevelType(Record record) throws OnRecordErrorException {
    for (String fieldPath : record.getEscapedFieldPaths()) {
      if (fieldPath.isEmpty()) {
        Field.Type type = record.get(fieldPath).getType();
        if (type != Field.Type.MAP && type != Field.Type.LIST_MAP) {
          throw new OnRecordErrorException(record, Errors.HBASE_29, type);
        }
        break;
      }
    }
  }

  protected byte[] getBytesForRowKey(Record record, StorageType rowKeyStorageType, String hbaseRowKey)
      throws OnRecordErrorException {
    byte[] value;
    Field field = record.get(hbaseRowKey);

    if (field == null) {
      throw new OnRecordErrorException(record, Errors.HBASE_27, hbaseRowKey);
    }

    if (rowKeyStorageType == StorageType.TEXT) {
      value = Bytes.toBytes(field.getValueAsString());
    } else {
      value = convertToBinary(field, record);
    }

    if (value.length == 0) {
      throw new OnRecordErrorException(record, Errors.HBASE_35);
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
        throw new OnRecordErrorException(record, Errors.HBASE_12, Field.Type.DATE.name(), StorageType.BINARY.name());
      case TIME:
        throw new OnRecordErrorException(record, Errors.HBASE_12, Field.Type.TIME.name(), StorageType.BINARY.name());
      case DATETIME:
        throw new OnRecordErrorException(record,
            Errors.HBASE_12,
            Field.Type.DATETIME.name(),
            StorageType.BINARY.name()
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
        throw new OnRecordErrorException(record, Errors.HBASE_12, Field.Type.LIST.name(), StorageType.BINARY.name());
      case LIST_MAP:
        throw new OnRecordErrorException(record,
            Errors.HBASE_12,
            Field.Type.LIST_MAP.name(),
            StorageType.BINARY.name()
        );
      case LONG:
        value = Bytes.toBytes(field.getValueAsLong());
        break;
      case MAP:
        throw new OnRecordErrorException(Errors.HBASE_12, Field.Type.MAP.name(), StorageType.BINARY.name(), record);
      case SHORT:
        value = Bytes.toBytes(field.getValueAsShort());
        break;
      case STRING:
        throw new OnRecordErrorException(record, Errors.HBASE_12, Field.Type.STRING.name(), StorageType.BINARY.name());
      default:
        throw new FieldConversionException("This shouldn't happen: Conversion not defined for " + field.toString());
    }
    return value;
  }

  private byte[] getBytesForValue(
      Record record, String fieldPath, StorageType storageType, boolean ignoreMissingField
  ) throws OnRecordErrorException {
    StorageType columnStorageType = storageType;
    byte[] value;
    Field field = record.get(fieldPath);
    if (field == null || field.getValue() == null) {
      if (!ignoreMissingField) {
        throw new OnRecordErrorException(record, Errors.HBASE_25, fieldPath);
      } else {
        return new byte[0];
      }
    }
    // column storage type is null for implicit field mapping
    if (columnStorageType == null) {
      columnStorageType = getColumnStorageType(field.getType());
    }
    // Figure the storage type and convert appropriately
    if (columnStorageType == (StorageType.TEXT)) {
      if (field.getType() == Field.Type.BYTE_ARRAY ||
          field.getType() == Field.Type.MAP ||
          field.getType() == Field.Type.LIST_MAP ||
          field.getType() == Field.Type.LIST) {
        throw new OnRecordErrorException(record, Errors.HBASE_12, field.getType(), StorageType.TEXT.name());
      } else {
        value = Bytes.toBytes(field.getValueAsString());
      }
    } else if (columnStorageType == StorageType.JSON_STRING) {
      // only map and list can be converted to json string
      if (field.getType() == Field.Type.MAP ||
          field.getType() == Field.Type.LIST ||
          field.getType() == Field.Type.LIST_MAP) {
        try {
          value = JsonUtil.jsonRecordToBytes(((ContextExtensions) getContext()), record, field);
        } catch (StageException se) {
          throw new OnRecordErrorException(record,
              Errors.HBASE_31,
              field.getType(),
              StorageType.JSON_STRING.getLabel(),
              se
          );
        }
      } else {
        throw new OnRecordErrorException(record, Errors.HBASE_12, field.getType(), StorageType.JSON_STRING.name());
      }
    } else {
      value = convertToBinary(field, record);
    }
    return value;
  }

  @Override
  public void validateQuorumConfigs(List<Stage.ConfigIssue> issues) {
    HBaseConnectionHelper.validateQuorumConfigs(issues,
        getContext(),
        Groups.HBASE.name(),
        hBaseConnectionConfig.zookeeperQuorum,
        hBaseConnectionConfig.zookeeperParentZNode,
        hBaseConnectionConfig.clientPort
    );
  }

  private StorageType getColumnStorageType(Field.Type fieldType) {
    StorageType storageType;
    switch (fieldType) {
      case BOOLEAN:
      case CHAR:
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case DATETIME:
      case TIME:
      case DECIMAL:
      case STRING:
        storageType = StorageType.TEXT;
        break;
      case BYTE_ARRAY:
        storageType = StorageType.BINARY;
        break;
      case MAP:
      case LIST:
      case LIST_MAP:
        storageType = StorageType.JSON_STRING;
        break;
      default:
        throw new FieldConversionException(Utils.format("Conversion not defined for: {} ", fieldType));
    }
    return storageType;
  }

  @Override
  public Date getRecordTime(Record record, String timeDriver) throws OnRecordErrorException {
    if (timeDriver.trim().isEmpty()) {
      return null;
    }

    try {
      ELVars variables = getContext().createELVars();
      TimeNowEL.setTimeNowInContext(variables, getBatchTime());
      RecordEL.setRecordInContext(variables, record);
      return (getContext().createELEval("timeDriver")).eval(variables, timeDriver, Date.class);
    } catch (ELEvalException e) {
      throw new OnRecordErrorException(Errors.HBASE_34, e);
    }
  }

  private void doExplicitFieldMapping(
      Put p, Record record, String timeDriver, Map<String, ColumnInfo> columnMappings, boolean ignoreMissingField
  ) throws OnRecordErrorException {
    Date recordTime = getRecordTime(record, timeDriver);
    for (Map.Entry<String, ColumnInfo> mapEntry : columnMappings.entrySet()) {
      HBaseColumn hbaseColumn = mapEntry.getValue().hbaseColumn;
      byte[] value = getBytesForValue(record, mapEntry.getKey(), mapEntry.getValue().storageType, ignoreMissingField);
      addCell(p, hbaseColumn.getCf(), hbaseColumn.getQualifier(), recordTime, value);
    }
  }

  private void doImplicitFieldMapping(
      Put p,
      Record record,
      StringBuilder errorMsgBuilder,
      Set<String> explicitFields,
      String timeDriver,
      boolean ignoreMissingField,
      boolean ignoreInvalidColumn,
      String hbaseRowKey
  ) throws OnRecordErrorException {
    validateRootLevelType(record);
    Date recordTime = getRecordTime(record, timeDriver);
    for (String fieldPath : record.getEscapedFieldPaths()) {
      if (!fieldPath.isEmpty() && !fieldPath.equals(hbaseRowKey) && !explicitFields.contains(fieldPath)) {
        String fieldPathColumn = fieldPath;
        if (fieldPath.charAt(0) == '/') {
          fieldPathColumn = fieldPath.substring(1);
        }
        HBaseColumn hbaseColumn = hbaseConnectionHelper.getColumn(fieldPathColumn.replace("'", ""));
        if (hbaseColumn != null) {
          byte[] value = getBytesForValue(record, fieldPath, null, ignoreMissingField);
          addCell(p, hbaseColumn.getCf(), hbaseColumn.getQualifier(), recordTime, value);
        } else if (ignoreInvalidColumn) {
          String errorMessage = Utils.format(Errors.HBASE_28.getMessage(),
              fieldPathColumn,
              KeyValue.COLUMN_FAMILY_DELIMITER
          );
          LOG.warn(errorMessage);
          errorMsgBuilder.append(errorMessage);
        } else {
          throw new OnRecordErrorException(record, Errors.HBASE_28, fieldPathColumn, KeyValue.COLUMN_FAMILY_DELIMITER);
        }
      }
    }
  }

  protected Put getHBasePut(
      Record record,
      byte[] rowKeyBytes,
      String timeDriver,
      Map<String, ColumnInfo> columnMappings,
      boolean ignoreMissingField,
      boolean implicitFieldMapping,
      boolean ignoreInvalidColumn,
      String hbaseRowKey
  ) throws OnRecordErrorException {
    Put p = new Put(rowKeyBytes);
    doExplicitFieldMapping(p, record, timeDriver, columnMappings, ignoreMissingField);
    if (implicitFieldMapping) {
      StringBuilder errorMsgBuilder = new StringBuilder();
      doImplicitFieldMapping(p,
          record,
          errorMsgBuilder,
          columnMappings.keySet(),
          timeDriver,
          ignoreMissingField,
          ignoreInvalidColumn,
          hbaseRowKey
      );
      if (p.isEmpty()) { // no columns in the Put; throw exception will all messages
        throw new OnRecordErrorException(record, Errors.HBASE_30, errorMsgBuilder.toString());
      }
    }
    return p;
  }

  @Override
  public Date setBatchTime() {
    batchTime = new Date();
    return batchTime;
  }

  private Date getBatchTime() {
    return batchTime;
  }
}
