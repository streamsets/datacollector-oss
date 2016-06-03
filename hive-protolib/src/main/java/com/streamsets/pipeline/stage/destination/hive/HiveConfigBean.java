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
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.Groups;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveConfigBean {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreTarget.class);

  @ConfigDef(
      required = true,
      label = "Hive JDBC URL",
      type = ConfigDef.Type.STRING,
      description = "Hive JDBC URL",
      defaultValue = "jdbc:hive2://<host>:<port>/" +
          "${record:value('"+ HiveMetastoreUtil.SEP + HiveMetastoreUtil.DATABASE_FIELD+"')}",
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
      label = "Hive JDBC Driver Name",
      description = "The Fully Qualifed Hive JDBC Drive Class Name",
      displayPosition = 20,
      group = "HIVE"
  )
  public String hiveJDBCDriver;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "/etc/hive/conf",
      label = "Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load core-site.xml, hdfs-site.xml and" +
          " hive-site.xml files to configure the Hive Metastore.",
      displayPosition = 20,
      group = "HIVE"
  )
  public String confDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional Hadoop Configuration",
      description = "Additional configuration properties. Values here override values loaded from config files.",
      displayPosition = 90,
      group = "ADVANCED"
  )
  public Map<String, String> additionalConfigProperties;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Hive Metastore Cache Size",
      description = "Cache Size",
      displayPosition = 30,
      group = "ADVANCED"
  )
  public long maxCacheSize = -1L;

  private static final Joiner JOINER = Joiner.on(".");

  /**
   * After init() it will contain merged configuration from all configured sources.
   */
  private Configuration configuration;
  public Configuration getConfiguration() {
    return configuration;
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

    // Try to connect to HMS to validate if the URL is valid
    Record dummyRecord = context.createRecord("DummyHiveMetastoreTargetRecord");
    Map<String, Field> databaseFieldValue = new HashMap<>();
    databaseFieldValue.put(HiveMetastoreUtil.DATABASE_FIELD, Field.create("default"));
    dummyRecord.set(Field.create(databaseFieldValue));
    String jdbcUrl = null;
    try {
      ELEval elEval = context.createELEval("hiveJDBCUrl");
      ELVars elVars = elEval.createVariables();
      RecordEL.setRecordInContext(elVars, dummyRecord);
      jdbcUrl = HiveMetastoreUtil.resolveEL(elEval, elVars, hiveJDBCUrl);
    } catch (ELEvalException e) {
      LOG.error("Error evaluating EL:", e);
      issues.add(context.createConfigIssue(
        Groups.HIVE.name(),
        JOINER.join(prefix, "hiveJDBCUrl"),
        Errors.HIVE_01,
        e.getMessage()
      ));
      return;
    }

    try (Connection con = DriverManager.getConnection(jdbcUrl)) {}
    catch (SQLException e) {
      LOG.error(Utils.format("Error Connecting to Hive Default Database with URL {}", jdbcUrl), e);
      issues.add(context.createConfigIssue(
        Groups.HIVE.name(),
        JOINER.join(prefix, "hiveJDBCUrl"),
        Errors.HIVE_22,
        jdbcUrl,
        e.getMessage()
      ));
    }
  }

}
