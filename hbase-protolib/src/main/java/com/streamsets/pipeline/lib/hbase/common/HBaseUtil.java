/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.lib.hbase.common;

import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class HBaseUtil {
  private HBaseUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);
  // master and region server principals are not defined in HBase constants, so do it here
  private static final String MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
  private static final String REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
  private static final String HBASE_CONF_DIR_CONFIG = "hbaseConfDir";
  private static UserGroupInformation loginUgi;

  public static Configuration getHBaseConfiguration(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      String HBaseName,
      String hbaseConfDir,
      String zookeeperQuorum,
      String zookeeperParentZnode,
      int clientPort,
      String tableName,
      boolean kerberosAuth,
      Map<String, String> hbaseConfigs
  ) {
    Configuration hbaseConf = HBaseConfiguration.create();
    if (hbaseConfDir != null && !hbaseConfDir.isEmpty()) {
      File hbaseConfigDir = new File(hbaseConfDir);

      if ((context.getExecutionMode() == ExecutionMode.CLUSTER_BATCH || context.getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
          || context.getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING) && hbaseConfigDir.isAbsolute()) {
        //Do not allow absolute hdfs config directory in cluster mode
        issues.add(
            context.createConfigIssue(HBaseName, HBASE_CONF_DIR_CONFIG, Errors.HBASE_24, hbaseConfDir)
        );
      } else {
        if (!hbaseConfigDir.isAbsolute()) {
          hbaseConfigDir = new File(context.getResourcesDirectory(), hbaseConfDir).getAbsoluteFile();
        }
        if (!hbaseConfigDir.exists()) {
          issues.add(context.createConfigIssue(HBaseName, HBASE_CONF_DIR_CONFIG, Errors.HBASE_19,
              hbaseConfDir));
        } else if (!hbaseConfigDir.isDirectory()) {
          issues.add(context.createConfigIssue(HBaseName, HBASE_CONF_DIR_CONFIG, Errors.HBASE_20,
              hbaseConfDir));
        } else {
          File hbaseSiteXml = new File(hbaseConfigDir, "hbase-site.xml");
          if (hbaseSiteXml.exists()) {
            if (!hbaseSiteXml.isFile()) {
              issues.add(context.createConfigIssue(
                  HBaseName,
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

    if (context.isPreview()) {
      // by default the retry number is set to 35 which is too much for preview mode
      LOG.debug("Setting HBase client retries to 3 for preview");
      hbaseConf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "3");
    }

    if (tableName == null || tableName.isEmpty()) {
      issues.add(context.createConfigIssue(HBaseName, "tableName", Errors.HBASE_05));
    }



    return hbaseConf;
  }

  public static void validateQuorumConfigs(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      String HBaseName,
      String zookeeperQuorum,
      String zookeeperParentZnode,
      int clientPort
      //String tableName
  ) {
    if (zookeeperQuorum == null || zookeeperQuorum.isEmpty()) {
      issues.add(context.createConfigIssue(HBaseName, "zookeeperQuorum",
          Errors.HBASE_04));
    } else {
      try {
        InetAddress.getByName(zookeeperQuorum);
      } catch (UnknownHostException ex) {
        issues.add(context.createConfigIssue(HBaseName, "zookeeperQuorum",
            Errors.HBASE_06, ex));
      }
    }
    if (zookeeperParentZnode == null || zookeeperParentZnode.isEmpty()) {
      issues.add(context.createConfigIssue(HBaseName, "zookeeperBaseDir",
          Errors.HBASE_09));
    }
    if (clientPort == 0) {
      issues.add(context.createConfigIssue(HBaseName, "clientPort", Errors.HBASE_13));

    }

  }

  public static void validateSecurityConfigs(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      String HBaseName,
      Configuration hbaseConf,
      boolean kerberosAuth
  ) {
    try {
      if (kerberosAuth) {
        hbaseConf.set(User.HBASE_SECURITY_CONF_KEY, UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        hbaseConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        String defaultRealm = null;
        if (hbaseConf.get(MASTER_KERBEROS_PRINCIPAL) == null) {
          try {
            defaultRealm = HadoopSecurityUtil.getDefaultRealm();
          } catch (Exception e) {
            issues.add(context.createConfigIssue(HBaseName, "masterPrincipal", Errors.HBASE_22));
          }
          hbaseConf.set(MASTER_KERBEROS_PRINCIPAL, "hbase/_HOST@" + defaultRealm);

        }
        if (hbaseConf.get(REGIONSERVER_KERBEROS_PRINCIPAL) == null) {
          try {
            if (defaultRealm == null) {
              defaultRealm = HadoopSecurityUtil.getDefaultRealm();
            }
          } catch (Exception e) {
            issues.add(context.createConfigIssue(HBaseName, "regionServerPrincipal", Errors.HBASE_23));
          }
          hbaseConf.set(REGIONSERVER_KERBEROS_PRINCIPAL, "hbase/_HOST@" + defaultRealm);
        }
      }

      loginUgi = HadoopSecurityUtil.getLoginUser(hbaseConf);

      StringBuilder logMessage = new StringBuilder();
      if (kerberosAuth) {
        logMessage.append("Using Kerberos");
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(context.createConfigIssue(HBaseName, "kerberosAuth", Errors.HBASE_16,
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
      issues.add(context.createConfigIssue(HBaseName, null, Errors.HBASE_17, ex.toString(), ex));
    }
  }

  public static UserGroupInformation getUGI(String hbaseUser) {
    return (hbaseUser == null || hbaseUser.isEmpty()) ?
        loginUgi : HadoopSecurityUtil.getProxyUser(hbaseUser, loginUgi);
  }

  public static HTableDescriptor checkConnectionAndTableExistence(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      Configuration hbaseConf,
      String HBaseName,
      String tableName
  ) throws Exception {
    LOG.debug("Validating connection to hbase cluster and whether table " + tableName + " exists and is enabled");
    HBaseAdmin hbaseAdmin = null;
    HTableDescriptor hTableDescriptor = null;
    try {
      hbaseAdmin = new HBaseAdmin(hbaseConf);
      if (!hbaseAdmin.tableExists(tableName)) {
        issues.add(context.createConfigIssue(HBaseName, "tableName", Errors.HBASE_07, tableName));
      } else if (!hbaseAdmin.isTableEnabled(tableName)) {
        issues.add(context.createConfigIssue(HBaseName, "tableName", Errors.HBASE_08, tableName));
      } else {
        hTableDescriptor = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
      }
    } catch (Exception ex) {
      LOG.warn("Received exception while connecting to cluster: ", ex);
      issues.add(context.createConfigIssue(HBaseName, null, Errors.HBASE_06, ex.toString(), ex));
    } finally {
      if (hbaseAdmin != null) {
        try {
          hbaseAdmin.close();
        } catch (IOException ex) {
          throw ex;
        }
      }
    }
    return hTableDescriptor;
  }

  public static void handleNoColumnFamilyException(
      Exception rex,
      Iterator<Record> records,
      ErrorRecordHandler errorRecordHandler
  ) throws StageException {
    // Column is null or No such Column Family exception
    if(rex.getCause() instanceof NullPointerException
      || rex.getCause() instanceof NoSuchColumnFamilyException
      || rex.getCause().getCause() instanceof NoSuchColumnFamilyException
    ) {
      while(records.hasNext()) {
        Record record = records.next();
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.HBASE_37, rex.getCause()));
      }
    } else {
      throw new StageException(Errors.HBASE_36, rex);
    }
  }

  public static void handleNoColumnFamilyException(
    RetriesExhaustedWithDetailsException rex,
    Record record,
    Map<String, Record> rowKeyToRecord,
    ErrorRecordHandler errorRecordHandler
  ) throws StageException {
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

  public static void setIfNotNull(Configuration conf, String property, String value) {
    if(value != null) {
      conf.set(property, value);
    }
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

  public static HBaseColumn getColumn(String column) {
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
}
