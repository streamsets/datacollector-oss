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
package com.streamsets.pipeline.stage.lib.hive;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HiveConfigBean.class);
  private static final String KERBEROS_JDBC_REGEX = "jdbc:.*;principal=.*@.*";
  private  static final String HIVE_JDBC_URL = "hiveJDBCUrl";

  @ConfigDef(
      required = true,
      label = "JDBC URL",
      type = ConfigDef.Type.STRING,
      description = "JDBC URL used to connect to Hive." +
          "Use a valid JDBC URL format, such as: jdbc:hive2://<host>:<port>/<dbname>.",
      defaultValue = "jdbc:hive2://<host>:<port>/" +
          "${record:attribute('" + HiveMetastoreUtil.DATABASE_FIELD + "')}",
      displayPosition= 10,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      group = "HIVE"
  )
  public String hiveJDBCUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "org.apache.hive.jdbc.HiveDriver",
      label = "JDBC Driver Name",
      description = "The fully-qualified JDBC driver class name",
      displayPosition = 20,
      group = "HIVE"
  )
  public String hiveJDBCDriver;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "/etc/hive/conf",
      label = "Hadoop Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load core-site.xml," +
          " hdfs-site.xml and hive-site.xml files.",
      displayPosition = 30,
      group = "HIVE"
  )
  public String confDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional Hadoop Configuration",
      description = "Additional configuration properties. Values here override values loaded from config files.",
      displayPosition = 40,
      group = "HIVE"
  )
  public Map<String, String> additionalConfigProperties;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Max Cache Size (entries)",
      description = "Configures the cache size for storing table related information." +
          " Use -1 for unlimited number of table entries in the cache.",
      displayPosition = 60,
      group = "ADVANCED"
  )
  public long maxCacheSize = -1L;

  private static final Joiner JOINER = Joiner.on(".");

  /**
   * After init() it will contain merged configuration from all configured sources.
   */
  private Configuration configuration;
  private UserGroupInformation loginUgi;
  private ELEval elEval;

  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * This is for testing purpose
   * @param config: Configuration to set
   */
  public void setConfiguration(Configuration config) {
    configuration = config;
  }
  public UserGroupInformation getUgi() {
    return loginUgi;
  }

  public ELEval getElEval() {
    return elEval;
  }

  /**
   * Initialize and validate configuration options.
   */
  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    // Load JDBC driver
    try {
      Class.forName(hiveJDBCDriver);
    } catch (ClassNotFoundException e) {
      issues.add(context.createConfigIssue(
          Groups.HIVE.name(),
          JOINER.join(prefix, "hiveJDBCDriver"),
          Errors.HIVE_15,
          hiveJDBCDriver
      ));
    }

    // Prepare configuration object
    File hiveConfDir = new File(confDir);
    if (!hiveConfDir.isAbsolute()) {
      hiveConfDir = new File(context.getResourcesDirectory(), confDir).getAbsoluteFile();
    }

    configuration = new Configuration();

    if (hiveConfDir.exists()) {
      HiveMetastoreUtil.validateConfigFile("core-site.xml", confDir, hiveConfDir, issues, configuration, context);
      HiveMetastoreUtil.validateConfigFile("hdfs-site.xml", confDir, hiveConfDir, issues, configuration, context);
      HiveMetastoreUtil.validateConfigFile("hive-site.xml", confDir, hiveConfDir, issues, configuration, context);
    } else {
      issues.add(context.createConfigIssue(
          Groups.HIVE.name(),
          JOINER.join(prefix, "confDir"),
          Errors.HIVE_07,
          confDir
      ));
    }

    // Add any additional configuration overrides
    for (Map.Entry<String, String> entry : additionalConfigProperties.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    if(!issues.isEmpty()) {
      return;
    }

    elEval = context.createELEval(HIVE_JDBC_URL);

    // Try to connect to HMS to validate if the URL is valid
    Record dummyRecord = context.createRecord("DummyHiveMetastoreTargetRecord");
    Map<String, Field> databaseFieldValue = new HashMap<>();
    databaseFieldValue.put(HiveMetastoreUtil.DATABASE_FIELD, Field.create("default"));
    dummyRecord.set(Field.create(databaseFieldValue));
    String jdbcUrl;
    try {
      jdbcUrl = HiveMetastoreUtil.resolveJDBCUrl(elEval, hiveJDBCUrl, dummyRecord);
    } catch (ELEvalException e) {
      LOG.error("Error evaluating EL:", e);
      issues.add(context.createConfigIssue(
          Groups.HIVE.name(),
          JOINER.join(prefix, HIVE_JDBC_URL),
          Errors.HIVE_01,
          e.getMessage()
      ));
      return;
    }
    try {
      loginUgi = HadoopSecurityUtil.getLoginUser(configuration);
    } catch (Exception e) {
      issues.add(
          context.createConfigIssue(
              Groups.HIVE.name(),
              JOINER.join(prefix, HIVE_JDBC_URL),
              Errors.HIVE_22,
              jdbcUrl,
              e.getMessage()
          )
      );
      return;
    }
    try {
      if (jdbcUrl.matches(KERBEROS_JDBC_REGEX)) {
        LOG.info("Authentication: Kerberos");
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(
              context.createConfigIssue(
                  Groups.ADVANCED.name(),
                  JOINER.join(prefix, HIVE_JDBC_URL),
                  Errors.HIVE_01,
                  loginUgi.getAuthenticationMethod(),
                  UserGroupInformation.AuthenticationMethod.KERBEROS
              )
          );
        }
      } else {
        LOG.info("Authentication: Simple");
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
            UserGroupInformation.AuthenticationMethod.SIMPLE.name());
      }
    } catch (Exception ex) {
      LOG.info("Validation Error: " + ex.toString(), ex);
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              JOINER.join(prefix, HIVE_JDBC_URL),
              Errors.HIVE_01,
              "Exception in configuring HDFS"
          )
      );
      return;
    }

    HiveQueryExecutor executor = new HiveQueryExecutor(jdbcUrl, loginUgi);

    try (Connection con = executor.getConnection()) {} catch (Exception e) {
      LOG.error(Utils.format("Error Connecting to Hive Default Database with URL {}", jdbcUrl), e);
      issues.add(context.createConfigIssue(
          Groups.HIVE.name(),
          JOINER.join(prefix, HIVE_JDBC_URL),
          Errors.HIVE_22,
          jdbcUrl,
          e.getMessage()
      ));
    }
  }
}
