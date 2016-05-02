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
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ExecutionMode;
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
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.stage.destination.lib.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.lib.ErrorRecordHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
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
  // master and region server principals are not defined in HBase constants, so do it here
  private static final String MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
  private static final String REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
  private static final String HBASE_CONF_DIR_CONFIG = "hbaseConfDir";

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
  private final boolean ignoreMissingFieldPath;
  private final boolean ignoreInvalidColumn;
  private final String timeDriver;
  private Configuration hbaseConf;
  private UserGroupInformation loginUgi;
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
      boolean ignoreMissingFieldPath,
      boolean ignoreInvalidColumn,
      String timeDriver
  ) {
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
    this.ignoreMissingFieldPath = ignoreMissingFieldPath;
    this.ignoreInvalidColumn = ignoreInvalidColumn;
    this.timeDriver = timeDriver;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    hbaseConf = getHBaseConfiguration(issues);

    if (getContext().isPreview()) {
      // by default the retry number is set to 35 which is too much for preview mode
      LOG.debug("Setting HBase client retries to 3 for preview");
      hbaseConf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "3");
    }
    validateQuorumConfigs(issues);
    validateSecurityConfigs(issues);
    HTableDescriptor hTableDescriptor = null;
    if (issues.isEmpty()) {
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, this.zookeeperQuorum);
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, this.clientPort);
      hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.zookeeperParentZnode);
      hTableDescriptor = checkConnectionAndTableExistence(issues, this.tableName);
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
        } else if (!families.contains(hbaseColumn.cf)) {
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

  private Configuration getHBaseConfiguration(List<ConfigIssue> issues) {
    Configuration hbaseConf = HBaseConfiguration.create();
    if (hbaseConfDir != null && !hbaseConfDir.isEmpty()) {
      File hbaseConfigDir = new File(hbaseConfDir);
      if((getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH || getContext().getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
          || getContext().getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING) && hbaseConfigDir.isAbsolute()) {
        //Do not allow absolute hdfs config directory in cluster mode
        issues.add(
            getContext().createConfigIssue(Groups.HBASE.name(), HBASE_CONF_DIR_CONFIG, Errors.HBASE_24, hbaseConfDir)
        );
      } else {
        if (!hbaseConfigDir.isAbsolute()) {
          hbaseConfigDir = new File(getContext().getResourcesDirectory(), hbaseConfDir).getAbsoluteFile();
        }
        if (!hbaseConfigDir.exists()) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), HBASE_CONF_DIR_CONFIG, Errors.HBASE_19,
            hbaseConfDir));
        } else if (!hbaseConfigDir.isDirectory()) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), HBASE_CONF_DIR_CONFIG, Errors.HBASE_20,
            hbaseConfDir));
        } else {
          File hbaseSiteXml = new File(hbaseConfigDir, "hbase-site.xml");
          if (hbaseSiteXml.exists()) {
            if (!hbaseSiteXml.isFile()) {
              issues.add(getContext().createConfigIssue(
                      Groups.HBASE.name(),
                      HBASE_CONF_DIR_CONFIG,
                      Errors.HBASE_21,
                      hbaseConfDir,
                      "hbase-site.xml"
                  )
              );
            }
            hbaseConf.addResource(new Path(hbaseSiteXml.getAbsolutePath()));
          }
        }
      }
    }
    for (Map.Entry<String, String> config : hbaseConfigs.entrySet()) {
      hbaseConf.set(config.getKey(), config.getValue());
    }
    return hbaseConf;
  }

  private void validateQuorumConfigs(List<ConfigIssue> issues) {
    if (this.zookeeperQuorum == null || this.zookeeperQuorum.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "zookeeperQuorum",
        Errors.HBASE_04));
    } else {
      try {
        InetAddress.getByName(this.zookeeperQuorum);
      } catch (UnknownHostException ex) {
        issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "zookeeperQuorum",
          Errors.HBASE_06, ex));
      }
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
        hbaseConf.set(User.HBASE_SECURITY_CONF_KEY, UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        hbaseConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        if (hbaseConf.get(MASTER_KERBEROS_PRINCIPAL) == null) {
          try {
            hbaseConf.set(MASTER_KERBEROS_PRINCIPAL, "hbase/_HOST@" + KerberosUtil.getDefaultRealm());
          } catch (ReflectiveOperationException e) {
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "masterPrincipal", Errors.HBASE_22));
          }
        }
        if (hbaseConf.get(REGIONSERVER_KERBEROS_PRINCIPAL) == null) {
          try {
            hbaseConf.set(REGIONSERVER_KERBEROS_PRINCIPAL, "hbase/_HOST@" + KerberosUtil.getDefaultRealm());
          } catch (Exception e) {
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "regionServerPrincipal", Errors.HBASE_23));
          }
        }
      }

      UserGroupInformation.setConfiguration(hbaseConf);
      Subject subject = Subject.getSubject(AccessController.getContext());
      if (UserGroupInformation.isSecurityEnabled()) {
        loginUgi = UserGroupInformation.getUGIFromSubject(subject);
      } else {
        UserGroupInformation.loginUserFromSubject(subject);
        loginUgi = UserGroupInformation.getLoginUser();
      }
      LOG.info("Subject = {}, Principals = {}, Login UGI = {}", subject,
        subject == null ? "null" : subject.getPrincipals(), loginUgi);
      StringBuilder logMessage = new StringBuilder();
      if (kerberosAuth) {
        logMessage.append("Using Kerberos");
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "kerberosAuth", Errors.HBASE_16,
            loginUgi.getAuthenticationMethod()));
        }
      } else {
        logMessage.append("Using Simple");
        hbaseConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          UserGroupInformation.AuthenticationMethod.SIMPLE.name());
      }
      LOG.info("Authentication Config: " + logMessage);
    } catch (Exception ex) {
      LOG.info("Error validating security configuration: " + ex, ex);
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_17, ex.toString(), ex));
    }
  }

  private UserGroupInformation getUGI() {
    return (hbaseUser.isEmpty()) ? loginUgi : UserGroupInformation.createProxyUser(hbaseUser, loginUgi);
  }


  private HTableDescriptor checkConnectionAndTableExistence(final List<ConfigIssue> issues,
      final String tableName) {
    try {
      return getUGI().doAs(new PrivilegedExceptionAction<HTableDescriptor>() {
        @Override
        public HTableDescriptor run() throws Exception {
          LOG.debug("Validating connection to hbase cluster and whether table " + tableName + " exists and is enabled");
          HBaseAdmin hbaseAdmin = null;
          HTableDescriptor hTableDescriptor = null;
          try {
            HBaseAdmin.checkHBaseAvailable(hbaseConf);
            hbaseAdmin = new HBaseAdmin(hbaseConf);
            if (!hbaseAdmin.tableExists(tableName)) {
              issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "tableName", Errors.HBASE_07, tableName));
            } else if (!hbaseAdmin.isTableEnabled(tableName)) {
              issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "tableName", Errors.HBASE_08, tableName));
            } else {
              hTableDescriptor = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
            }
          } catch (Exception ex) {
            LOG.warn("Received exception while connecting to cluster: ", ex);
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_06, ex.toString(), ex));
          } finally {
            if (hbaseAdmin != null) {
              hbaseAdmin.close();
            }
          }
          return hTableDescriptor;
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
    StringBuilder errorMsgBuilder = new StringBuilder();
    doExplicitFieldMapping(p, record, errorMsgBuilder);
    if (implicitFieldMapping) {
      doImplicitFieldMapping(p, record, errorMsgBuilder, columnMappings.keySet());
    }
    if (p.isEmpty()) { // no columns in the Put; throw exception will all messages
      throw new OnRecordErrorException(record, Errors.HBASE_30,  errorMsgBuilder.toString());
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

  private void doExplicitFieldMapping(Put p, Record record, StringBuilder errorBuilder) throws OnRecordErrorException {
    Date recordTime = getRecordTime(record);
    for (Map.Entry<String, ColumnInfo> mapEntry : columnMappings.entrySet()) {
      HBaseColumn hbaseColumn = mapEntry.getValue().hbaseColumn;
      byte[] value = getBytesForValue(record, mapEntry.getKey(), mapEntry.getValue().storageType, errorBuilder);
      if (value != null) {
        addCell(p, hbaseColumn.cf, hbaseColumn.qualifier, recordTime, value);
      }
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
          byte[] value = getValueImplicitTypeMapping(record, fieldPath);
          addCell(p, hbaseColumn.cf, hbaseColumn.qualifier, recordTime, value);
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

  private byte[] getValueImplicitTypeMapping(Record record, String fieldPath) throws OnRecordErrorException {
    Field field = record.get(fieldPath);
    byte[] value;
    switch (field.getType()) {
      case BYTE_ARRAY:
        value = field.getValueAsByteArray();
        break;
      case BOOLEAN:
      case BYTE:
      case CHAR:
      case DATE:
      case DATETIME:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case INTEGER:
      case LONG:
      case SHORT:
      case STRING:
        value = Bytes.toBytes(field.getValueAsString());
        break;
      case LIST:
      case LIST_MAP:
      case MAP:
        try {
          value = JsonUtil.jsonRecordToBytes(record, field);
        } catch (StageException se) {
          throw new OnRecordErrorException(record, Errors.HBASE_31, field.getType(), StorageType.JSON_STRING.getLabel(), se);
        }
        break;
      default:
        throw new IllegalArgumentException("This shouldn't happen: " + "Conversion not defined for " + field.getType());
    }
    return value;
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
      getUGI().doAs(new PrivilegedExceptionAction<Void>() {
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
            handleNoColumnFamilyException(rex, record, null);
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
      handleNoColumnFamilyException(rex, null, rowKeyToRecord);
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

  private void handleNoColumnFamilyException(
    RetriesExhaustedWithDetailsException rex,
    Record record,
    Map<String, Record> rowKeyToRecord) throws StageException {
    for (int i = 0; i < rex.getNumExceptions(); i++) {
      if (rex.getCause(i) instanceof NoSuchColumnFamilyException) {
        Row r = rex.getRow(i);
        Record errorRecord = record != null ? record : rowKeyToRecord.get(Bytes.toString(r.getRow()));
        OnRecordErrorException exception =
          new OnRecordErrorException(errorRecord, Errors.HBASE_10,
            getErrorDescription(rex.getCause(i), r, rex.getHostnamePort(i)));
        errorRecordHandler.onError(exception);
      } else {
        // If at least 1 non NoSuchColumnFamilyException exception,
        // consider as stage exception
        throw new StageException(Errors.HBASE_02, rex);
      }
    }
  }

  private byte[] getBytesForValue(Record record, String fieldPath, StorageType columnStorageType, StringBuilder errorMsgBuilder) throws OnRecordErrorException {
    byte[] value;
    Field field = record.get(fieldPath);
    if (field == null) {
      if (!ignoreMissingFieldPath) {
        throw new OnRecordErrorException(record, Errors.HBASE_25, fieldPath);
      } else {
        String errorMessage = Utils.format(Errors.HBASE_25.getMessage(), fieldPath);
        LOG.warn(errorMessage);
        errorMsgBuilder.append(errorMessage);
        return null;
      }
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

 private static String getErrorDescription(Throwable t, Row row, String server) {
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

 private static class HBaseColumn {
    private byte[] cf;
    private byte[] qualifier;

    public HBaseColumn(byte[] cf, byte[] qualifier) {
      this.cf = cf;
      this.qualifier = qualifier;
    }
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
