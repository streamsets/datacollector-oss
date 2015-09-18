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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.jdbc.ChangeLogFormat;

import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "JDBC Producer",
    description = "Writes data to a JDBC destination.",
    upgrader = JdbcTargetUpgrader.class,
    icon = "rdbms.png"
)
@ConfigGroups(value = Groups.class)
public class JdbcDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JDBC Connection String",
      displayPosition = 10,
      group = "JDBC"
  )
  public String connectionString;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Additional JDBC Configuration Properties",
      description = "Additional properties to pass to the underlying JDBC driver.",
      displayPosition = 130,
      group = "JDBC"
  )
  public Map<String, String> driverProperties;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Credentials",
      defaultValue = "true",
      displayPosition = 25,
      group = "JDBC"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      defaultValue = "",
      displayPosition = 10,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Password",
      defaultValue = "",
      displayPosition = 20,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Table Name",
      description = "Depending on the database, may be specified as <schema>.<table>. Some databases require schema " +
          "be specified separately in the connection string.",
      displayPosition = 30,
      group = "JDBC"
  )
  public String tableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Column Mapping",
      description = "Optionally specify additional field mappings when input field name and column name don't match.",
      displayPosition = 40,
      group = "JDBC"
  )
  @ListBeanModel
  public List<JdbcFieldMappingConfig> columnNames;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Change Log Format",
      defaultValue = "NONE",
      description = "If input is a change data capture log, specify the format.",
      displayPosition = 50,
      group = "JDBC"
  )
  @ValueChooserModel(ChangeLogFormatChooserValues.class)
  public ChangeLogFormat changeLogFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Rollback Batch on Error",
      description = "Whether or not to rollback the entire batch on error. Some JDBC drivers provide information" +
          "about individual failed rows, and can insert partial batches.",
      displayPosition = 50,
      group = "JDBC"
  )
  public boolean rollbackOnError;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "JDBC Driver Class Name",
      description = "Class name for pre-JDBC 4 compliant drivers.",
      displayPosition = 140,
      group = "LEGACY"
  )
  public String driverClassName;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "Connection Health Test Query",
      description = "Not recommended for JDBC 4 compliant drivers. Runs when a new database connection is established.",
      displayPosition = 150,
      group = "LEGACY"
  )
  public String connectionTestQuery;

  @Override
  protected Target createTarget() {
    return new JdbcTarget(
        connectionString,
        username,
        password,
        tableName,
        columnNames,
        rollbackOnError,
        driverProperties,
        changeLogFormat,
        driverClassName,
        connectionTestQuery
    );
  }
}
