/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in
 * whole or part without written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Field.Type;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.JsonUtil;

public class HBaseTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTarget.class);
  final private String zookeeperQuorum;
  final private int clientPort;
  final private String zookeeperParentZnode;
  final private String tableName;
  final private String hbaseRowKey;
  final private List<HBaseFieldMappingConfig> hbaseFieldColumnMapping;
  final private boolean kerberosAuth;
  final private String kerberosPrincipal;
  final private SortedMap<String, ColumnInfo> columnMappings = new TreeMap<>();
  private Configuration hbaseConf;
  final private Map<String, String> hbaseConfigs;
  final private StorageType rowKeyStorageType;
  final private String kerberosKeytab;
  private UserGroupInformation ugi;

  public HBaseTarget(String zookeeperQuorum, int clientPort, String zookeeperParentZnode,
      String tableName, String hbaseRowKey, StorageType rowKeyStorageType,
      List<HBaseFieldMappingConfig> hbaseFieldColumnMapping, boolean kerberosAuth,
      String kerberosPrincipal, String kerberosKeytab, Map<String, String> hbaseConfigs) {
    this.zookeeperQuorum = zookeeperQuorum;
    this.clientPort = clientPort;
    this.zookeeperParentZnode = zookeeperParentZnode;
    this.tableName = tableName;
    this.hbaseRowKey = hbaseRowKey;
    this.hbaseFieldColumnMapping = hbaseFieldColumnMapping;
    this.kerberosAuth = kerberosAuth;
    this.kerberosPrincipal = kerberosPrincipal;
    this.hbaseConfigs = hbaseConfigs;
    this.rowKeyStorageType = rowKeyStorageType;
    this.kerberosKeytab = kerberosKeytab;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    for (HBaseFieldMappingConfig column : hbaseFieldColumnMapping) {
      columnMappings.put(column.columnName, new ColumnInfo(column.columnValue,
          column.columnStorageType));
    }
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    hbaseConf = HBaseConfiguration.create();
    for (Map.Entry<String, String> config : hbaseConfigs.entrySet()) {
      hbaseConf.set(config.getKey(), config.getValue());
    }
    validateQuorumConfigs(issues);
    validateSecurityConfigs(issues);
    if (issues.isEmpty()) {
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, this.zookeeperQuorum);
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, this.clientPort);
      hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.zookeeperParentZnode);
      checkConnectionAndTableExistence(issues, this.tableName);
    }
    validateStorageTypes(issues);
    return issues;
  }

  private void validateQuorumConfigs(List<ConfigIssue> issues) {
    if (this.zookeeperQuorum == null || this.zookeeperQuorum.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "zookeeperQuorum",
        Errors.HBASE_04));
    }
    if (this.zookeeperParentZnode == null || this.zookeeperParentZnode.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "zookeeperBaseDir",
        Errors.HBASE_09));
    }
    if (this.clientPort == 0) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "clientPort", Errors.HBASE_13));

    }
    if (this.tableName == null || this.tableName.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "tableName", Errors.HBASE_05));

    }
  }

  private void validateSecurityConfigs(List<ConfigIssue> issues) {
    try {
      if (kerberosAuth) {
        hbaseConf.set(User.HBASE_SECURITY_CONF_KEY,
          UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        UserGroupInformation.setConfiguration(hbaseConf);
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
        if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "kerberosAuth",
            Errors.HBASE_16, ugi.getAuthenticationMethod()));
        }
      } else {
        hbaseConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          UserGroupInformation.AuthenticationMethod.SIMPLE.name());
        ugi = UserGroupInformation.getLoginUser();
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_17,
        ex.getMessage(), ex));
    }
  }

  private void checkConnectionAndTableExistence(final List<ConfigIssue> issues,
      final String tableName) {
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            LOG.debug("Validating connection to hbase cluster and whether table " + tableName
                + " exists and is enabled");
            HBaseAdmin.checkHBaseAvailable(hbaseConf);
            HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
            if (!hbaseAdmin.tableExists(tableName)) {
              issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_07,
                tableName));
            } else if (!hbaseAdmin.isTableEnabled(tableName)) {
              issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_08,
                tableName));
            }
            hbaseAdmin.close();
          } catch (Exception ex) {
            LOG.warn("Received exception while connecting to cluster: ", ex);
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_06,
              ex.getMessage(), ex));
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.warn("Unexpected exception", e);
      throw new RuntimeException(e);
    }
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

    if (hbaseFieldColumnMapping == null || hbaseFieldColumnMapping.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "hbaseFieldColumnMapping",
        Errors.HBASE_18));
    } else {
      for (HBaseFieldMappingConfig hbaseFieldMappingConfig : hbaseFieldColumnMapping) {
        switch (hbaseFieldMappingConfig.columnStorageType) {
        case BINARY:
        case JSON_STRING:
        case TEXT:
          break;
        default:
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "columnStorageType",
            Errors.HBASE_15, hbaseFieldMappingConfig.columnStorageType));
        }
      }
    }
  }

  private Put getHBasePut(Record record, byte[] rowKeyBytes) throws StageException {
    Put p = new Put(rowKeyBytes);
    for (Map.Entry<String, ColumnInfo> mapEntry : columnMappings.entrySet()) {
      // Parse the column in column family and qualifier
      byte[][] parts = KeyValue.parseColumn(Bytes.toBytes(mapEntry.getKey()));
      byte[] cf;
      byte[] qualifier;
      if (parts.length == 1) {
        cf = parts[0];
        // empty qualifier is ok
        qualifier = Bytes.toBytes(StringUtils.EMPTY);
      } else if (parts.length == 2) {
        cf = parts[0];
        qualifier = parts[1];
      } else {
        throw new OnRecordErrorException(Errors.HBASE_11, record, mapEntry.getKey());
      }
      byte[] value = getBytesForValue(record, mapEntry.getValue());
      p.add(cf, qualifier, value);
    }
    return p;
  }

  private byte[] getBytesForRowKey(Record record) throws OnRecordErrorException {
    byte[] value;
    Field field = record.get(this.hbaseRowKey);
    if (rowKeyStorageType == StorageType.TEXT) {
      value = Bytes.toBytes(field.getValueAsString());
    } else {
      value = convertToBinary(field, record);
    }
    return value;
  }

  @Override
  public void write(final Batch batch) throws StageException {
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          writeBatch(batch);
          return null;
        }
      });
    } catch (IOException e) {
      throw new StageException(Errors.HBASE_02, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while processing batch of records at HBase destination", e);
      throw new RuntimeException(e);
    }
  }

  private void writeBatch(Batch batch) throws StageException {
    HTable hTable = null;
    Iterator<Record> it = batch.getRecords();
    Map<String, Record> rowKeyToRecord = new HashMap<String, Record>();
    Map<Record, WriteErrorInfo> badRecordsInfo = new HashMap<Record, WriteErrorInfo>();
    try {
      hTable = new HTable(hbaseConf, tableName);
      while (it.hasNext()) {
        Record record = it.next();
        try {
          byte[] rowKeyBytes = getBytesForRowKey(record);
          // Map hbase rows to sdc records.
          rowKeyToRecord.put(Bytes.toString(rowKeyBytes), record);
          Put p = getHBasePut(record, rowKeyBytes);
          try {
            // HTable internally keeps a buffer, a put() will keep on buffering till the buffer
            // limit is reached
            // Once it hits the buffer limit or autoflush is set to true, commit will happen
            hTable.put(p);
          } catch (RetriesExhaustedWithDetailsException ex) {
            // There may be more than one row which failed to persist
            for (int i = 0; i < ex.getNumExceptions(); i++) {
              Row r = ex.getRow(i);
              // Map of bad records -> the error description
              badRecordsInfo.put(rowKeyToRecord.get(Bytes.toString(r.getRow())),
                new WriteErrorInfo(ex.getCause(i), r, ex.getHostnamePort(i)));
            }
            throw new StageException(Errors.HBASE_02, ex);
          } catch (InterruptedIOException ex) {
            throw new StageException(Errors.HBASE_02, ex);
          }
        } catch (Exception ex) {
          LOG.debug("Got exception while writing to hbase", ex);
          switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            if (badRecordsInfo.isEmpty()) {
              getContext().toError(record, ex);
            } else {
              for (Map.Entry<Record, WriteErrorInfo> badRecordEntry : badRecordsInfo.entrySet()) {
                getContext().toError(badRecordEntry.getKey(), Errors.HBASE_10, record, badRecordEntry.getValue().getDescription());
              }
              badRecordsInfo.clear();
            }
            break;
          case STOP_PIPELINE:
            throw ex;
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
              getContext().getOnErrorRecord(), ex));
          }
        }
      }
      // This will flush the internal buffer
      hTable.flushCommits();
    } catch (Exception ex) {
      LOG.debug("Got exception while writing to hbase", ex);
      switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        if (ex instanceof RetriesExhaustedWithDetailsException) {
          RetriesExhaustedWithDetailsException retriesEx = ((RetriesExhaustedWithDetailsException) ex);
          LOG.warn("Received error while writing records to Hbase destination " + retriesEx.getExhaustiveDescription());
          for (int i = 0; i < retriesEx.getNumExceptions(); i++) {
            Row r = retriesEx.getRow(i);
            Record record = rowKeyToRecord.get(Bytes.toString(r.getRow()));
            getContext().toError(record, Errors.HBASE_10, record,
              new WriteErrorInfo(retriesEx.getCause(i), r, retriesEx.getHostnamePort(i)).getDescription());
          }
        } else {
          throw new StageException(Errors.HBASE_02, ex);
        }
        break;
      case STOP_PIPELINE:
        throw new StageException(Errors.HBASE_02, ex);
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
          getContext().getOnErrorRecord(), ex));
      }
    } finally {
      try {
        if (hTable != null) {
          hTable.close();
        }
      } catch (IOException e) {
        LOG.warn("Cannot close htable ", e);
        hTable = null;
      }
    }
  }

  private byte[] getBytesForValue(Record record, ColumnInfo columnInfo) throws StageException {
    byte[] value = null;
    String index = columnInfo.columnValue;
    StorageType columnStorageType = columnInfo.storageType;
    Field field = record.get(index);
    // Figure the storage type and convert appropriately
    if (columnStorageType == (StorageType.TEXT)) {
      value = Bytes.toBytes(field.getValueAsString());
    } else if (columnStorageType == StorageType.JSON_STRING) {
      // only map and list can be converted to json string
      if (field.getType() == Type.MAP || field.getType() == Type.LIST) {
        value = JsonUtil.jsonRecordToBytes(record, field);
      } else {
        throw new OnRecordErrorException(Errors.HBASE_12, field.getType(),
            StorageType.JSON_STRING.name(), record);
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
      throw new OnRecordErrorException(Errors.HBASE_12, Type.DATE.name(),
          StorageType.BINARY.name(), record);
    case DATETIME:
      throw new OnRecordErrorException(Errors.HBASE_12, Type.DATETIME.name(),
          StorageType.BINARY.name(), record);
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
      throw new OnRecordErrorException(Errors.HBASE_12, Type.LIST.name(),
          StorageType.BINARY.name(), record);
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
      throw new OnRecordErrorException(Errors.HBASE_12, Type.STRING.name(),
          StorageType.BINARY.name(), record);
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
    private ColumnInfo(String columnValue, StorageType hbaseStorageType) {
      this.columnValue = columnValue;
      this.storageType = hbaseStorageType;
    }

    private String columnValue;
    private StorageType storageType;
  }

  private static class WriteErrorInfo {
    private Throwable t;
    private Row row;
    private String server;

    private WriteErrorInfo(Throwable t, Row row, String server) {
      this.t = t;
      this.row = row;
      this.server = server;
    }

    private String getDescription() {
      StringWriter errorWriter = new StringWriter();
      PrintWriter pw = new PrintWriter(errorWriter);
      pw.append("Exception from " + server + " for " + Bytes.toStringBinary(row.getRow()));
      if (t != null) {
        pw.println();
        t.printStackTrace(pw);
      }
      pw.flush();
      return errorWriter.toString();
    }
  }
}
