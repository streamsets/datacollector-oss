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
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.JsonUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.File;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
  private final SortedMap<String, ColumnInfo> columnMappings = new TreeMap<>();
  private final Map<String, String> hbaseConfigs;
  private final StorageType rowKeyStorageType;
  private final String hbaseConfDir;
  private final String hbaseUser;

  private Configuration hbaseConf;
  private UserGroupInformation loginUgi;

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
      String hbaseUser
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
    if (issues.isEmpty()) {
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, this.zookeeperQuorum);
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, this.clientPort);
      hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.zookeeperParentZnode);
      checkConnectionAndTableExistence(issues, this.tableName);
    }
    validateStorageTypes(issues);
    if (issues.isEmpty()) {
      for (HBaseFieldMappingConfig column : hbaseFieldColumnMapping) {
        columnMappings.put(column.columnName, new ColumnInfo(column.columnValue, column.columnStorageType));
      }
    }
    return issues;
  }

  private Configuration getHBaseConfiguration(List<ConfigIssue> issues) {
    Configuration hbaseConf = HBaseConfiguration.create();
    if (hbaseConfDir != null && !hbaseConfDir.isEmpty()) {
      File hbaseConfigDir = new File(hbaseConfDir);
      if((getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH || getContext().getExecutionMode() == ExecutionMode.CLUSTER_STREAMING) && hbaseConfigDir.isAbsolute()) {
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
          } catch (Exception e) {
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


  private void checkConnectionAndTableExistence(final List<ConfigIssue> issues,
      final String tableName) {
    try {
      getUGI().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          LOG.debug("Validating connection to hbase cluster and whether table " + tableName + " exists and is enabled");
          HBaseAdmin hbaseAdmin = null;
          try {
            HBaseAdmin.checkHBaseAvailable(hbaseConf);
            hbaseAdmin = new HBaseAdmin(hbaseConf);
            if (!hbaseAdmin.tableExists(tableName)) {
              issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_07, tableName));
            } else if (!hbaseAdmin.isTableEnabled(tableName)) {
              issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_08, tableName));
            }
          } catch (Exception ex) {
            LOG.warn("Received exception while connecting to cluster: ", ex);
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_06, ex.toString(), ex));
          } finally {
            if (hbaseAdmin != null) {
              hbaseAdmin.close();
            }
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

  private Put getHBasePut(Record record, byte[] rowKeyBytes) throws OnRecordErrorException, StageException {
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
    if (field == null) {
      throw new OnRecordErrorException(Errors.HBASE_27, this.hbaseRowKey);
    }
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
          rowKeyToRecord.put(Bytes.toString(rowKeyBytes), record);
          Put p = getHBasePut(record, rowKeyBytes);
          try {
            // HTable internally keeps a buffer, a put() will keep on buffering till the buffer
            // limit is reached
            // Once it hits the buffer limit or autoflush is set to true, commit will happen
            hTable.put(p);
          } catch (Exception ex) {
            throw new StageException(Errors.HBASE_02, ex);
          }
        } catch (OnRecordErrorException ex) {
          LOG.debug("Got exception while writing to HBase", ex);
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().toError(record, ex);
              break;
            case STOP_PIPELINE:
              throw ex;
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'", getContext()
                .getOnErrorRecord(), ex));
          }
        }
      }
      // This will flush the internal buffer
      hTable.flushCommits();
    } catch (Exception ex) {
      LOG.debug("Got exception while flushing commits to HBase", ex);
      throw new StageException(Errors.HBASE_02, ex);
    } finally {
      try {
        if (hTable != null) {
          hTable.close();
        }
      } catch (IOException e) {
        LOG.warn("Cannot close table ", e);
      }
    }
  }

  private byte[] getBytesForValue(Record record, ColumnInfo columnInfo) throws OnRecordErrorException, StageException {
    byte[] value;
    String index = columnInfo.columnValue;
    StorageType columnStorageType = columnInfo.storageType;
    Field field = record.get(index);
    if (field == null) {
      throw new OnRecordErrorException(Errors.HBASE_25, index);
    }
    // Figure the storage type and convert appropriately
    if (columnStorageType == (StorageType.TEXT)) {
      value = Bytes.toBytes(field.getValueAsString());
    } else if (columnStorageType == StorageType.JSON_STRING) {
      // only map and list can be converted to json string
      if (field.getType() == Type.MAP || field.getType() == Type.LIST || field.getType() == Type.LIST_MAP) {
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
    case LIST_MAP:
      throw new OnRecordErrorException(Errors.HBASE_12, Type.LIST_MAP.name(),
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

    private final String columnValue;
    private final StorageType storageType;
  }

}
