/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.el.OffsetEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeActionChooserValues;

@StageDef(
    version = 10,
    label = "JDBC Query Consumer",
    description = "Reads data from a JDBC source using a query.",
    icon = "rdbms.png",
    execution = ExecutionMode.STANDALONE,
    upgrader = JdbcSourceUpgrader.class,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    onlineHelpRefUrl = "index.html#Origins/JDBCConsumer.html#task_ryz_tkr_bs"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@HideConfigs({
    "commonSourceConfigBean.allowLateTable",
    "commonSourceConfigBean.enableSchemaChanges",
    "commonSourceConfigBean.queriesPerSecond"
})
public class JdbcDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Incremental Mode",
      description = "Disabling Incremental Mode will always substitute the value in" +
          " Initial Offset in place of ${OFFSET} instead of the most recent value of <offsetColumn>.",
      displayPosition = 15,
      group = "JDBC"
  )
  public boolean isIncrementalMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "SQL Query",
      description =
          "SELECT <offset column>, ... FROM <table name> WHERE <offset column>  >  ${OFFSET} ORDER BY <offset column>",
      elDefs = {OffsetEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 20,
      group = "JDBC"
  )
  public String query;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Initial Offset",
      description = "Initial value to insert for ${offset}." +
          " Subsequent queries will use the result of the Next Offset Query",
      displayPosition = 40,
      group = "JDBC"
  )
  public String initialOffset;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Offset Column",
      description = "Column checked to track current offset.",
      displayPosition = 50,
      group = "JDBC"
  )
  public String offsetColumn;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LIST_MAP",
      label = "Root Field Type",
      displayPosition = 130,
      group = "JDBC"
  )
  @ValueChooserModel(JdbcRecordTypeChooserValues.class)
  public JdbcRecordType jdbcRecordType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${10 * SECONDS}",
      label = "Query Interval",
      displayPosition = 140,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "JDBC"
  )
  public long queryInterval;

  @ConfigDefBean
  public CommonSourceConfigBean commonSourceConfigBean;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Transaction ID Column Name",
      description = "When reading a change data table, column identifying the transaction the change belongs to.",
      displayPosition = 180,
      group = "CDC"
  )
  public String txnIdColumnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Transaction Size",
      description = "If transactions exceed this size, they will be applied in multiple batches.",
      defaultValue = "10000",
      displayPosition = 190,
      group = "CDC"
  )
  public int txnMaxSize;

  @ConfigDefBean()
  public HikariPoolConfigBean hikariConfigBean;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Create JDBC Header Attributes",
      description = "Generates record header attributes that provide additional details about source data, such as the original data type or source table name.",
      defaultValue = "true",
      displayPosition = 200,
      group = "ADVANCED"
  )
  public boolean createJDBCNsHeaders = true;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "JDBC Header Prefix",
      description = "Prefix for the header attributes, used as follows: <prefix>.<field name>.<type of information>. For example: jdbc.<field name>.precision and jdbc.<field name>.scale",
      defaultValue = "jdbc.",
      displayPosition = 210,
      group = "ADVANCED",
      dependsOn = "createJDBCNsHeaders",
      triggeredByValue = "true"
  )
  public String jdbcNsHeaderPrefix = "jdbc.";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Disable Query Validation",
      description = "Disables the validation query and does not validate query formatting such as " +
          "presence of ${OFFSET} or ORDER BY clause.",
      defaultValue = "false",
      displayPosition = 220,
      group = "ADVANCED"
  )
  public boolean disableValidation = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "On Unknown Type",
      description = "Action that should be performed when an unknown type is detected in the result set.",
      defaultValue = "STOP_PIPELINE",
      displayPosition = 230,
      group = "ADVANCED"
  )
  @ValueChooserModel(UnknownTypeActionChooserValues.class)
  public UnknownTypeAction unknownTypeAction = UnknownTypeAction.STOP_PIPELINE;

  @Override
  protected Source createSource() {
    return new JdbcSource(
        isIncrementalMode,
        query,
        initialOffset,
        offsetColumn,
        disableValidation,
        txnIdColumnName,
        txnMaxSize,
        jdbcRecordType,
        commonSourceConfigBean,
        createJDBCNsHeaders,
        jdbcNsHeaderPrefix,
        hikariConfigBean,
        unknownTypeAction,
        queryInterval
      );
  }
}
