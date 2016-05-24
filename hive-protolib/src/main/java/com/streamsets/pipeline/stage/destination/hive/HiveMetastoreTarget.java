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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.HMSCacheType;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.HiveType;
import com.streamsets.pipeline.stage.lib.hive.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.TypeInfoCacheSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveMetastoreTarget extends BaseTarget{
  private static final String CONF = "conf";
  private static final String HIVE_CONFIG_BEAN = "hiveConfigBean";
  private static final String HIVE_JDBC_URL = "hiveJDBCUrl";
  private static final String HIVE_JDBC_DRIVER = "hiveJDBCDriver";
  private static final String HDFS_KERBEROS = "hdfsKerberos";
  private static final String CONF_DIR = "confDir";
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreTarget.class.getCanonicalName());
  private static final Joiner JOINER = Joiner.on(".");

  private final HMSTargetConfigBean conf;

  private ErrorRecordHandler defaultErrorRecordHandler;
  private ELEval elEval;
  private HMSCache hmsCache;

  //Only initialized if useAsAvro is false.
  private FileSystem fs;
  private UserGroupInformation loginUgi;

  public HiveMetastoreTarget(HMSTargetConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    try {
      Class.forName(conf.hiveConfigBean.hiveJDBCDriver);
    } catch (ClassNotFoundException e) {
      issues.add(
          getContext().createConfigIssue(
              Groups.HIVE.name(),
              JOINER.join(CONF, HIVE_CONFIG_BEAN, HIVE_JDBC_DRIVER),
              Errors.HIVE_15,
              conf.hiveConfigBean.hiveJDBCDriver
          )
      );
    }

    initConfDirAndHDFS(issues);

    if (issues.isEmpty()) {
      defaultErrorRecordHandler = new DefaultErrorRecordHandler(getContext());
      elEval = getContext().createELEval(HIVE_JDBC_URL);
      hmsCache =  HMSCache.newCacheBuilder()
          .addCacheTypeSupport(
              Arrays.asList(
                  HMSCacheType.TYPE_INFO,
                  HMSCacheType.PARTITION_VALUE_INFO
              )
          )
          .maxCacheSize(conf.hiveConfigBean.maxCacheSize)
          .build();
      validateJDBCUrlWithDummyRecord(conf.hiveConfigBean.hiveJDBCUrl, issues);
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> recordIterator = batch.getRecords();
    while (recordIterator.hasNext()) {
      Record metadataRecord = recordIterator.next();
      try {
        String tableName = HiveMetastoreUtil.getTableName(metadataRecord);
        String databaseName = HiveMetastoreUtil.getDatabaseName(metadataRecord);
        String qualifiedTableName = HiveMetastoreUtil.getQualifiedTableName(databaseName, tableName);
        String resolvedJDBCUrl = resolveJDBCUrl(conf.hiveConfigBean.hiveJDBCUrl, metadataRecord);
        String location = HiveMetastoreUtil.getLocation(metadataRecord);
        HiveQueryExecutor hiveQueryExecutor = new HiveQueryExecutor(resolvedJDBCUrl);
        if (HiveMetastoreUtil.isSchemaChangeRecord(metadataRecord)) {
          handleSchemaChange(metadataRecord, qualifiedTableName, resolvedJDBCUrl, location, hiveQueryExecutor);
        } else {
          handlePartitionAddition(metadataRecord, qualifiedTableName, resolvedJDBCUrl, location, hiveQueryExecutor);
        }
      } catch (StageException e) {
        LOG.error("Error processing record: {}", e);
        defaultErrorRecordHandler.onError(new OnRecordErrorException(metadataRecord, e.getErrorCode(), e.getParams()));
      }
    }
  }

  @Override
  public void destroy() {
    try {
      loginUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (fs != null) {
            fs.close();
          }
          return null;
        }
      });
    }
    catch (Exception e) {
      LOG.warn("Error when closing hdfs file system:", e);
    }
    super.destroy();
  }

  private void initConfDirAndHDFS(final List<ConfigIssue> issues) {
    String hiveConfDirString = conf.hiveConfigBean.confDir;
    File hiveConfDir = new File(hiveConfDirString);
    final Configuration configuration = new Configuration();

    if (!hiveConfDir.isAbsolute()) {
      hiveConfDir = new File(getContext().getResourcesDirectory(), conf.hiveConfigBean.confDir).getAbsoluteFile();
    }

    if (hiveConfDir.exists()) {
      HiveMetastoreUtil.validateConfigFile("core-site.xml", hiveConfDirString,
          hiveConfDir, issues, configuration, getContext());

      HiveMetastoreUtil.validateConfigFile("hdfs-site.xml", hiveConfDirString,
          hiveConfDir, issues, configuration, getContext());

    } else {
      issues.add(
          getContext().createConfigIssue(
              Groups.HIVE.name(),
              JOINER.join(HiveMetastoreUtil.CONF, HiveMetastoreUtil.HIVE_CONFIG_BEAN, HiveMetastoreUtil.CONF_DIR),
              Errors.HIVE_07,
              hiveConfDirString
          )
      );
    }

    // Add any additional configuration overrides
    for (Map.Entry<String, String> entry : conf.hiveConfigBean.additionalConfigProperties.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    if (!issues.isEmpty()) {
      return;
    }

    if (!conf.useAsAvro) {
      try {
        // forcing UGI to initialize with the security settings from the stage
        loginUgi = HadoopSecurityUtil.getLoginUser(configuration);
        if (conf.hdfsKerberos) {
          LOG.info("HDFS Using Kerberos");
          if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.ADVANCED.name(),
                    JOINER.join(CONF, HDFS_KERBEROS),
                    Errors.HIVE_01,
                    loginUgi.getAuthenticationMethod(),
                    UserGroupInformation.AuthenticationMethod.KERBEROS
                )
            );
          }
        } else {
          LOG.info("HDFS Using Simple");
          configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
              UserGroupInformation.AuthenticationMethod.SIMPLE.name());
        }
      } catch (Exception ex) {
        LOG.info("Validation Error: " + ex.toString(), ex);
        issues.add(
            getContext().createConfigIssue(
                Groups.ADVANCED.name(),
                JOINER.join(CONF, HDFS_KERBEROS),
                Errors.HIVE_01,
                "Exception in configuring HDFS"
            )
        );
      }

      //use ugi.
      try {
        loginUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() {
            try {
              fs = FileSystem.get(configuration);
            } catch (IOException e) {
              LOG.error("Error with HDFS File System Configuration.", e);

            }
            return null;
          }
        });
      } catch (Exception e) {
        issues.add(
            getContext().createConfigIssue(
                Groups.HIVE.name(),
                JOINER.join(CONF, HIVE_CONFIG_BEAN, CONF_DIR),
                Errors.HIVE_01,
                e.getMessage()
            )
        );
      }
    }
  }
  private void validateJDBCUrlWithDummyRecord(String unResolvedJDBCUrl, List<ConfigIssue> issues){
    Record dummyRecord = getContext().createRecord("DummyHiveMetastoreTargetRecord");
    Map<String, Field> databaseFieldValue = new HashMap<>();
    databaseFieldValue.put(HiveMetastoreUtil.DATABASE_FIELD, Field.create("default"));
    dummyRecord.set(Field.create(databaseFieldValue));
    String jdbcUrl = null;
    try {
      jdbcUrl = resolveJDBCUrl(unResolvedJDBCUrl, dummyRecord);
    } catch (ELEvalException e) {
      LOG.error("Error evaluating EL:", e);
      issues.add(
          getContext().createConfigIssue(
              Groups.HIVE.name(),
              JOINER.join(HiveMetastoreUtil.CONF, HiveMetastoreUtil.HIVE_CONFIG_BEAN, HIVE_JDBC_URL),
              Errors.HIVE_01,
              e.getMessage()
          )
      );
      return;
    }
    try (Connection con = DriverManager.getConnection(jdbcUrl)) {}
    catch (SQLException e) {
      LOG.error(Utils.format("Error Connecting to Hive Default Database with URL {}", jdbcUrl), e);
      issues.add(
          getContext().createConfigIssue(
              Groups.HIVE.name(),
              JOINER.join(HiveMetastoreUtil.CONF, HiveMetastoreUtil.HIVE_CONFIG_BEAN, HIVE_JDBC_URL),
              Errors.HIVE_22,
              jdbcUrl,
              e.getMessage()
          )
      );
    }
  }

  private String resolveJDBCUrl(String unresolvedJDBCUrl, Record metadataRecord) throws ELEvalException {
    ELVars elVars = elEval.createVariables();
    RecordEL.setRecordInContext(elVars, metadataRecord);
    return elEval.eval(elVars, unresolvedJDBCUrl, String.class);
  }

  private void handleSchemaChange(
      Record metadataRecord,
      String qualifiedTableName,
      String resolvedJDBCUrl,
      String location,
      HiveQueryExecutor hiveQueryExecutor
  ) throws StageException {
    //Schema Change
    HMSCacheType cacheType = HMSCacheType.TYPE_INFO;
    TypeInfoCacheSupport.TypeInfo cachedColumnTypeInfo = hmsCache.getOrLoad(
        cacheType,
        resolvedJDBCUrl, qualifiedTableName
    );
    LinkedHashMap<String, HiveType> newColumnTypeInfo = HiveMetastoreUtil.getColumnNameType(metadataRecord);
    LinkedHashMap<String, HiveType> partitionTypeInfo = HiveMetastoreUtil.getPartitionNameType(metadataRecord);
    boolean isInternal = HiveMetastoreUtil.getInternalField(metadataRecord);
    String schemaPath = null;

    if (!conf.useAsAvro) {
      //TODO: RegenerateSchema with the Hive Structure, SDC-2988
      String avroSchema = HiveMetastoreUtil.getAvroSchema(metadataRecord);
      schemaPath = HiveMetastoreUtil.serializeSchemaToHDFS(loginUgi, fs, location, avroSchema);
    }

    if (cachedColumnTypeInfo == null) {
      //Create Table
      hiveQueryExecutor.executeCreateTableQuery(
          qualifiedTableName,
          newColumnTypeInfo,
          partitionTypeInfo,
          conf.useAsAvro,
          schemaPath,
          isInternal
      );

      hmsCache.put(
          cacheType,
          qualifiedTableName,
          new TypeInfoCacheSupport.TypeInfo(newColumnTypeInfo, partitionTypeInfo)
      );
    } else {
      //Add Columns
      LinkedHashMap<String, HiveType> columnDiff = cachedColumnTypeInfo.getDiff(newColumnTypeInfo);
      if (!columnDiff.isEmpty()) {
        hiveQueryExecutor.executeAlterTableAddColumnsQuery(qualifiedTableName, columnDiff);
        if (!conf.useAsAvro) {
          hiveQueryExecutor.executeAlterTableSetTblPropertiesQuery(qualifiedTableName, schemaPath);
        }
        cachedColumnTypeInfo.updateState(columnDiff);
      }
    }
  }

  private void handlePartitionAddition(
      Record metadataRecord,
      String qualifiedTableName,
      String resolvedJDBCUrl,
      String location,
      HiveQueryExecutor hiveQueryExecutor
  ) throws StageException {
    //Partition Addition
    TypeInfoCacheSupport.TypeInfo cachedTypeInfo = hmsCache.getOrLoad(
        HMSCacheType.TYPE_INFO,
        resolvedJDBCUrl, qualifiedTableName
    );

    HMSCacheType hmsCacheType = HMSCacheType.PARTITION_VALUE_INFO;
    PartitionInfoCacheSupport.PartitionInfo cachedPartitionInfo = hmsCache.getOrLoad(
        hmsCacheType,
        resolvedJDBCUrl, qualifiedTableName
    );
    LinkedHashMap<String, String> partitionValMap = HiveMetastoreUtil.getPartitionNameValue(metadataRecord);
    Set<LinkedHashMap <String, String>> partitionInfoDiff =
        new LinkedHashSet<>(Collections.singletonList(partitionValMap));
    partitionInfoDiff = (cachedPartitionInfo != null)?
        cachedPartitionInfo.getDiff(partitionInfoDiff) : partitionInfoDiff;
    if (!partitionInfoDiff.isEmpty()) {
      hiveQueryExecutor.executeAlterTableAddPartitionQuery(
          qualifiedTableName,
          partitionValMap,
          cachedTypeInfo.getPartitionTypeInfo(),
          location
      );
      if (cachedPartitionInfo != null) {
        cachedPartitionInfo.updateState(partitionInfoDiff);
      } else {
        hmsCache.put(hmsCacheType, qualifiedTableName, new PartitionInfoCacheSupport.PartitionInfo(partitionInfoDiff));
      }
    }
  }
}
