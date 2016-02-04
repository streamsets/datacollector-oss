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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.ChangeLogFormat;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;

import java.util.List;

@HideConfigs(value = {"hikariConfigBean.readOnly"})
@GenerateResourceBundle
@StageDef(
    version = 5,
    label = "JDBC Producer",
    description = "Writes data to a JDBC destination.",
    upgrader = JdbcTargetUpgrader.class,
    icon = "rdbms.png",
    onlineHelpRefUrl = "index.html#Destinations/JDBCProducer.html#task_cx3_lhh_ht"
)
@ConfigGroups(value = Groups.class)
public class JdbcDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${record:attribute('tableName')}",
      label = "Table Name",
      description = "Depending on the database, may be specified as <schema>.<table>. Some databases require schema " +
          "be specified separately in the connection string.",
      displayPosition = 30,
      group = "JDBC"
  )
  public String tableNameTemplate;

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
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Multi-Row Insert",
      description = "Whether to generate multi-row INSERT statements instead of batches of single-row INSERTs",
      displayPosition = 60,
      group = "JDBC"
  )
  public boolean useMultiRowInsert;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Statement Parameter Limit",
      description = "The maximum number of prepared statement parameters allowed in each batch insert statement when " +
          "using multi-row inserts. Set to -1 to disable limit.",
      dependsOn = "useMultiRowInsert",
      triggeredByValue = "true",
      displayPosition = 60,
      group = "JDBC"
  )
  public int maxPrepStmtParameters;

  @ConfigDefBean()
  public HikariPoolConfigBean hikariConfigBean;

  @Override
  protected Target createTarget() {
    return new JdbcTarget(
        tableNameTemplate,
        columnNames,
        rollbackOnError,
        useMultiRowInsert,
        maxPrepStmtParameters,
        changeLogFormat,
        hikariConfigBean
    );
  }
}
