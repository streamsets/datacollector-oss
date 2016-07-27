/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.protobuf.ServiceException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Field.Type;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.hbase.common.Errors;
import com.streamsets.pipeline.lib.hbase.common.HBaseColumn;
import com.streamsets.pipeline.lib.hbase.common.HBaseUtil;
import com.streamsets.pipeline.lib.util.JsonUtil;

import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTarget.class);

  private final String zookeeperQuorum;
  private final int clientPort;
  private final String zookeeperParentZnode;
  private final String tableName;
  private final String hbaseRowKey;
  private final List<HBaseFieldMappingConfig> hbaseFieldColumnMapping;
  private final boolean kerberosAuth;
  private final Map<String, ColumnInfo> columnMappings = new HashMap<>();
  private final Map<String, String> hbaseConfigs;
  private final StorageType rowKeyStorageType;
  private final String hbaseConfDir;
  private final String hbaseUser;
  private final boolean implicitFieldMapping;
  private final boolean ignoreMissingField;
  private final boolean ignoreInvalidColumn;
  private final String timeDriver;
  private Configuration hbaseConf;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval timeDriverElEval;
  private Date batchTime;

  public HBaseTarget(
    String zookeeperQuorum,
    int clientPort,
    String zookeeperParentZnode,
    String tableName,
    String hbaseRowKey,
    StorageType rowKeyStorageType,
    List<HBaseFieldMappingConfig> hbaseFieldColumnMapping,
    boolean kerberosAuth,
    String hbaseConfDir,
    Map<String, String> hbaseConfigs,
    String hbaseUser,
    boolean implicitFieldMapping,
    boolean ignoreMissingField,
    boolean ignoreInvalidColumn,
    String timeDriver
  ) {
    if (null != zookeeperQuorum) {
      zookeeperQuorum = CharMatcher.WHITESPACE.removeFrom(zookeeperQuorum);
    }
    this.zookeeperQuorum = zookeeperQuorum;
    this.clientPort = clientPort;
    this.zookeeperParentZnode = zookeeperParentZnode;
    this.tableName = tableName;
    this.hbaseRowKey = hbaseRowKey;
    this.hbaseFieldColumnMapping = hbaseFieldColumnMapping;
    this.kerberosAuth = kerberosAuth;
    this.hbaseConfigs = hbaseConfigs;
    this.rowKeyStorageType = rowKeyStorageType;
    this.hbaseConfDir = hbaseConfDir;
    this.hbaseUser = hbaseUser;
    this.implicitFieldMapping = implicitFieldMapping;
    this.ignoreMissingField = ignoreMissingField;
    this.ignoreInvalidColumn = ignoreInvalidColumn;
    this.timeDriver = timeDriver;
  }

  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();
    hbaseConf = HBaseUtil.getHBaseConfiguration(
      issues,
      getContext(),
      Groups.HBASE.name(),
      hbaseConfDir,
      zookeeperQuorum,
      zookeeperParentZnode,
      clientPort,
      tableName,
      kerberosAuth,
      hbaseConfigs
    );

    validateQuorumConfigs(issues);
    HBaseUtil.validateSecurityConfigs(issues, getContext(), Groups.HBASE.name(), hbaseConf, kerberosAuth);

    if(issues.isEmpty()) {
      HBaseUtil.setIfNotNull(hbaseConf, HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, clientPort);
      HBaseUtil.setIfNotNull(hbaseConf, HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperParentZnode);
    }

    HTableDescriptor hTableDescriptor = null;
    if (issues.isEmpty()) {
      try {
        hTableDescriptor = HBaseUtil.getUGI(hbaseUser).doAs(new PrivilegedExceptionAction<HTableDescriptor>() {
          @Override
          public HTableDescriptor run() throws Exception {
          try {
            checkHBaseAvailable(hbaseConf);
          } catch (Exception ex) {
            LOG.warn("Received exception while connecting to cluster: ", ex);
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_06, ex.toString(), ex));
          }
          return HBaseUtil.checkConnectionAndTableExistence(issues, getContext(), hbaseConf, Groups.HBASE.name(), tableName);
          }
        });
      } catch(InterruptedException | IOException e) {
        LOG.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }

    if (!timeDriver.trim().isEmpty()) {
      timeDriverElEval = getContext().createELEval("timeDriver");
      try {
        setBatchTime();
        getRecordTime(getContext().createRecord("validateTimeDriver"));
      } catch (OnRecordErrorException ex) {
        // OREE is just a wrapped ElEvalException, so unwrap this for the error message
        issues.add(getContext().createConfigIssue(
          Groups.HBASE.name(),
          "timeDriverEval",
          Errors.HBASE_33,
          ex.getCause().toString(),
          ex.getCause()
        ));
      }
    }

    validateStorageTypes(issues);
    if (issues.isEmpty()) {
      Collection<byte[]> families = hTableDescriptor.getFamiliesKeys();
      for (HBaseFieldMappingConfig column : hbaseFieldColumnMapping) {
        HBaseColumn hbaseColumn = getColumn(column.columnName);
        if (hbaseColumn == null) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "hbaseFieldColumnMapping", Errors.HBASE_28,
            column.columnName, KeyValue.COLUMN_FAMILY_DELIMITER));
        } else if (!families.contains(hbaseColumn.getCf())) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "hbaseFieldColumnMapping", Errors.HBASE_32,
            column.columnName, this.tableName));
        } else {
          columnMappings.put(column.columnValue, new ColumnInfo(hbaseColumn, column.columnStorageType));
        }
      }
    }
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return issues;
  }

  protected void validateQuorumConfigs(List<ConfigIssue> issues) {
    HBaseUtil.validateQuorumConfigs(issues, getContext(), Groups.HBASE.name(), zookeeperQuorum, zookeeperParentZnode, clientPort);
  }

  protected void checkHBaseAvailable(Configuration conf) throws IOException, ServiceException {
    HBaseAdmin.checkHBaseAvailable(conf);
  }

  private void validateStorageTypes(List<ConfigIssue> issues) {
    switch (this.rowKeyStorageType) {
      case BINARY:
      case TEXT:
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "rowKeyStorageType",
          Errors.HBASE_14, rowKeyStorageType));
    }

    if (!hbaseFieldColumnMapping.isEmpty()) {
      for (HBaseFieldMappingConfig hbaseFieldMappingConfig : hbaseFieldColumnMapping) {
        switch (hbaseFieldMappingConfig.columnStorageType) {
          case BINARY:
          case JSON_STRING:
          case TEXT:
            break;
          default:
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "columnStorageType", Errors.HBASE_15,
              hbaseFieldMappingConfig.columnStorageType));
        }
      }
    } else if (!implicitFieldMapping) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "hbaseFieldColumnMapping", Errors.HBASE_18));
    }
  }

  private HBaseColumn getColumn(String column) {
    byte[][] parts = KeyValue.parseColumn(Bytes.toBytes(column));
    byte[] cf = null;
    byte[] qualifier = null;
    if (parts.length == 2) {
      cf = parts[0];
      qualifier = parts[1];
      return new HBaseColumn(cf, qualifier);
    } else {
      return null;
    }
  }

  private Put getHBasePut(Record record, byte[] rowKeyBytes) throws OnRecordErrorException, ELEvalException {
    Put p = new Put(rowKeyBytes);
    doExplicitFieldMapping(p, record);
    if (implicitFieldMapping) {
      StringBuilder errorMsgBuilder = new StringBuilder();
      doImplicitFieldMapping(p, record, errorMsgBuilder, columnMappings.keySet());
      if (p.isEmpty()) { // no columns in the Put; throw exception will all messages
        throw new OnRecordErrorException(record, Errors.HBASE_30, errorMsgBuilder.toString());
      }
    }
    return p;
  }

  @VisibleForTesting
  Date setBatchTime() {
    batchTime = new Date();
    return batchTime;
  }

  private Date getBatchTime() {
    return batchTime;
  }

  private Date getRecordTime(Record record) throws OnRecordErrorException {
    if (timeDriver.trim().isEmpty()) {
      return null;
    }

    try {
      ELVars variables = getContext().createELVars();
      TimeNowEL.setTimeNowInContext(variables, getBatchTime());
      RecordEL.setRecordInContext(variables, record);
      return timeDriverElEval.eval(variables, timeDriver, Date.class);
    } catch (ELEvalException e) {
      throw new OnRecordErrorException(Errors.HBASE_34, e);
    }
  }

  private void doExplicitFieldMapping(Put p, Record record) throws OnRecordErrorException {
    Date recordTime = getRecordTime(record);
    for (Map.Entry<String, ColumnInfo> mapEntry : columnMappings.entrySet()) {
      HBaseColumn hbaseColumn = mapEntry.getValue().hbaseColumn;
      byte[] value = getBytesForValue(record, mapEntry.getKey(), mapEntry.getValue().storageType);
      addCell(p, hbaseColumn.getCf(), hbaseColumn.getQualifier(), recordTime, value);
    }
  }

  private void addCell(Put p, byte[] columnFamily, byte[] qualifier, Date recordTime, byte[] value) {
    if (recordTime != null) {
      p.add(columnFamily, qualifier, recordTime.getTime(), value);
    } else {
      p.add(columnFamily, qualifier, value);
    }
  }

  private void validateRootLevelType(Record record) throws OnRecordErrorException {
    for (String fieldPath : record.getEscapedFieldPaths()) {
      if (fieldPath.isEmpty()) {
        Type type = record.get(fieldPath).getType();
        if (type != Type.MAP && type != Type.LIST_MAP) {
          throw new OnRecordErrorException(record, Errors.HBASE_29, type);
        }
        break;
      }
    }
  }

  private void doImplicitFieldMapping(Put p, Record record, StringBuilder errorMsgBuilder, Set<String> explicitFields)
      throws OnRecordErrorException {
    validateRootLevelType(record);
    Date recordTime = getRecordTime(record);
    for (String fieldPath : record.getEscapedFieldPaths()) {
      if (!fieldPath.isEmpty() && !fieldPath.equals(this.hbaseRowKey) && !explicitFields.contains(fieldPath)) {
        String fieldPathColumn = fieldPath;
        if (fieldPath.charAt(0) == '/') {
          fieldPathColumn = fieldPath.substring(1);
        }
        HBaseColumn hbaseColumn = getColumn(fieldPathColumn.replace("'", ""));
        if (hbaseColumn != null) {
          byte[] value = getBytesForValue(record, fieldPath, null);
          addCell(p, hbaseColumn.getCf(), hbaseColumn.getQualifier(), recordTime, value);
        } else {
          if (ignoreInvalidColumn) {
            String errorMessage = Utils.format(Errors.HBASE_28.getMessage(), fieldPathColumn, KeyValue.COLUMN_FAMILY_DELIMITER);
            LOG.warn(errorMessage);
            errorMsgBuilder.append(errorMessage);
          } else {
            throw new OnRecordErrorException(record, Errors.HBASE_28, fieldPathColumn, KeyValue.COLUMN_FAMILY_DELIMITER);
          }
        }
      }
    }
  }

  private byte[] getBytesForRowKey(Record record) throws OnRecordErrorException {
    byte[] value;
    Field field = record.get(this.hbaseRowKey);

    if (field == null) {
      throw new OnRecordErrorException(record, Errors.HBASE_27, this.hbaseRowKey);
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

  @Override
  public void write(final Batch batch) throws StageException {
    setBatchTime();
    try {
      HBaseUtil.getUGI(hbaseUser).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
        writeBatch(batch);
        return null;
        }
      });
    } catch (Exception e) {
      throw throwStageException(e);
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.HBASE_26, cause, cause);
      }
    }
    return new StageException(Errors.HBASE_26, e, e);
  }

  private void writeBatch(Batch batch) throws StageException {
    HTable hTable = null;
    Iterator<Record> it = batch.getRecords();
    Map<String, Record> rowKeyToRecord = new HashMap<>();
    try {
      hTable = new HTable(hbaseConf, tableName);
      // Disable auto-flush to increase performance by reducing the number of RPCs.
      // HTable is deprecated as of HBase 1.0 and replaced by Table which does not use autoFlush
      hTable.setAutoFlushTo(false);
      while (it.hasNext()) {
        Record record = it.next();
        try {
          byte[] rowKeyBytes = getBytesForRowKey(record);
          // Map hbase rows to sdc records.
          Put p = getHBasePut(record, rowKeyBytes);
          rowKeyToRecord.put(Bytes.toString(rowKeyBytes), record);
          try {
            // HTable internally keeps a buffer, a put() will keep on buffering till the buffer
            // limit is reached
            // Once it hits the buffer limit or autoflush is set to true, commit will happen
            hTable.put(p);
          } catch (RetriesExhaustedWithDetailsException rex) {
            // There may be more than one row which failed to persist
            HBaseUtil.handleNoColumnFamilyException(rex, record, null, errorRecordHandler);
          }
        } catch (OnRecordErrorException ex) {
          LOG.debug("Got exception while writing to HBase", ex);
          errorRecordHandler.onError(ex);
        }
      }
      // This will flush the internal buffer
      hTable.flushCommits();
    } catch (RetriesExhaustedWithDetailsException rex) {
      LOG.debug("Got exception while flushing commits to HBase", rex);
      HBaseUtil.handleNoColumnFamilyException(rex, null, rowKeyToRecord, errorRecordHandler);
    } catch (OnRecordErrorException ex) {
      LOG.debug("Got exception while writing to HBase", ex);
      errorRecordHandler.onError(ex);
    } catch (IOException ex) {
      LOG.debug("Got exception while flushing commits to HBase", ex);
      throw new StageException(Errors.HBASE_02, ex);
    }
    finally {
      try {
        if (hTable != null) {
          hTable.close();
        }
      } catch (IOException e) {
        LOG.warn("Cannot close table ", e);
      }
    }
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
        throw new RuntimeException(Utils.format("Conversion not defined for: {} ", fieldType));
    }
    return storageType;
  }

  private byte[] getBytesForValue(
      Record record, String fieldPath, StorageType columnStorageType
  ) throws OnRecordErrorException {
    byte[] value;
    Field field = record.get(fieldPath);
    if (field == null || field.getValue() == null) {
      if (!ignoreMissingField) {
        throw new OnRecordErrorException(record, Errors.HBASE_25, fieldPath);
      } else {
        return null;
      }
    }
    // column storage type is null for implicit field mapping
    if (columnStorageType == null) {
      columnStorageType = getColumnStorageType(field.getType());
    }
    // Figure the storage type and convert appropriately
    if (columnStorageType == (StorageType.TEXT)) {
      if (field.getType() == Type.BYTE_ARRAY
        || field.getType() == Type.MAP
        || field.getType() == Type.LIST_MAP
        || field.getType() == Type.LIST) {
        throw new OnRecordErrorException(record, Errors.HBASE_12, field.getType(),
          StorageType.TEXT.name());
      } else {
        value = Bytes.toBytes(field.getValueAsString());
      }
    } else if (columnStorageType == StorageType.JSON_STRING) {
      // only map and list can be converted to json string
      if (field.getType() == Type.MAP || field.getType() == Type.LIST || field.getType() == Type.LIST_MAP) {
        try {
          value = JsonUtil.jsonRecordToBytes(record, field);
        } catch (StageException se) {
          throw new OnRecordErrorException(record, Errors.HBASE_31, field.getType(), StorageType.JSON_STRING.getLabel(), se);
        }
      } else {
        throw new OnRecordErrorException(record, Errors.HBASE_12, field.getType(),
          StorageType.JSON_STRING.name());
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
        throw new OnRecordErrorException(record, Errors.HBASE_12, Type.DATE.name(),
            StorageType.BINARY.name());
      case TIME:
        throw new OnRecordErrorException(record, Errors.HBASE_12, Type.TIME.name(),
          StorageType.BINARY.name());
      case DATETIME:
        throw new OnRecordErrorException(record, Errors.HBASE_12, Type.DATETIME.name(),
            StorageType.BINARY.name());
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
        throw new OnRecordErrorException(record, Errors.HBASE_12, Type.LIST.name(),
          StorageType.BINARY.name());
      case LIST_MAP:
        throw new OnRecordErrorException(record, Errors.HBASE_12, Type.LIST_MAP.name(),
          StorageType.BINARY.name());
      case LONG:
        value = Bytes.toBytes(field.getValueAsLong());
        break;
      case MAP:
        throw new OnRecordErrorException(Errors.HBASE_12, Type.MAP.name(), StorageType.BINARY.name(),
          record);
      case SHORT:
        value = Bytes.toBytes(field.getValueAsShort());
        break;
      case STRING:
        throw new OnRecordErrorException(record, Errors.HBASE_12, Type.STRING.name(),
          StorageType.BINARY.name());
      default:
        throw new RuntimeException("This shouldn't happen: " + "Conversion not defined for "
          + field.toString());
    }
    return value;
  }

  @VisibleForTesting
  Configuration getHBaseConfiguration() {
    return hbaseConf;
  }

  private static class ColumnInfo {
    private final HBaseColumn hbaseColumn;
    private final StorageType storageType;

    private ColumnInfo(HBaseColumn hbaseColumn, StorageType hbaseStorageType) {
      this.hbaseColumn = hbaseColumn;
      this.storageType = hbaseStorageType;
    }
  }
}
