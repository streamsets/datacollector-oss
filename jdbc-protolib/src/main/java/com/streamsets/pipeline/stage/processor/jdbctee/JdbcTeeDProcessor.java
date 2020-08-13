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
package com.streamsets.pipeline.stage.processor.jdbctee;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.jdbc.JDBCOperationChooserValues;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.lib.jdbc.JDBCOperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationActionChooserValues;
import com.streamsets.pipeline.lib.operation.ChangeLogFormatChooserValues;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import java.util.List;

@StageDef(
    version = 4,
    label = "JDBC Tee",
    description = "Write records to JDBC and enrich records with generated columns",
    upgrader = JdbcTeeUpgrader.class,
    upgraderDef = "upgrader/JdbcTeeDProcessor.yaml",
    icon = "rdbms.png",
    onlineHelpRefUrl ="index.html?contextID=task_qpj_ncy_hw"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(value = {
  "hikariConfigBean.readOnly",
  "hikariConfigBean.autoCommit"
})
public class JdbcTeeDProcessor extends DProcessor {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Schema Name",
      displayPosition = 10,
      group = "JDBC"
  )
  public String schema;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${record:attribute('tableName')}",
      label = "Table Name",
      description = "Table Names should contain only table names. Schema should be defined in the connection string or " +
          "schema configuration",
      displayPosition = 20,
      group = "JDBC"
  )
  public String tableNameTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Column Mapping",
      description = "Optionally specify additional field mappings when input field name and column name don't match.",
      displayPosition = 30,
      group = "JDBC"
  )
  @ListBeanModel
  public List<JdbcFieldColumnParamMapping> customMappings;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Generated Column Mappings",
      defaultValue = "",
      description = "Mappings from generated columns to field names",
      displayPosition = 40,
      group = "JDBC"
  )
  @ListBeanModel
  public List<JdbcFieldColumnMapping> generatedColumnMappings;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enclose Table Name",
      description = "Use for lower or mixed-case database, table and field names. " +
          "Select only when the database or tables were created with quotation marks around the names.",
      displayPosition = 40,
      group = "JDBC",
      defaultValue = "false"
  )
  public boolean encloseTableName;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Change Log Format",
      defaultValue = "NONE",
      description = "If input is a change data capture log, specify the format.",
      displayPosition = 40,
      group = "JDBC"
  )
  @ValueChooserModel(ChangeLogFormatChooserValues.class)
  public ChangeLogFormat changeLogFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Default Operation",
      description = "Default operation to perform if sdc.operation.type is not set in record header.",
      displayPosition = 40,
      group = "JDBC"
  )
  @ValueChooserModel(JDBCOperationChooserValues.class)
  public JDBCOperationType defaultOperation;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue= "DISCARD",
      label = "Unsupported Operation Handling",
      description = "Action to take when operation type is not supported",
      displayPosition = 50,
      group = "JDBC"
  )
  @ValueChooserModel(UnsupportedOperationActionChooserValues.class)
  public UnsupportedOperationAction unsupportedAction;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Multi-Row Operation",
      description = "Whether to generate multi-row INSERT statements instead of batches of single-row INSERTs",
      displayPosition = 60,
      group = "JDBC"
  )
  public boolean useMultiRowOp;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Statement Parameter Limit",
      description = "The maximum number of prepared statement parameters allowed in each batch insert statement when " +
          "" + "using multi-row inserts. Set to -1 to disable limit.",
      dependsOn = "useMultiRowOp",
      triggeredByValue = "true",
      displayPosition = 60,
      group = "JDBC"
  )
  public int maxPrepStmtParameters = -1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Rollback Batch on Error",
      description = "Whether or not to rollback the entire batch on error. Some JDBC drivers provide information" +
          "about individual failed rows, and can insert partial batches.",
      displayPosition = 70,
      group = "JDBC"
  )
  public boolean rollbackOnError;

  @ConfigDefBean()
  public JdbcHikariPoolConfigBean hikariConfigBean;

  /**
   * Returns the Hikari config bean.
   * <p/>
   * This method is used to pass the Hikari config bean to the underlaying connector.
   * <p/>
   * Subclasses may override this method to provide specific vendor configurations.
   * <p/>
   * IMPORTANT: when a subclass is overriding this method to return a specialized HikariConfigBean, the config property
   * itself in the connector subclass must have the same name as the config property in this class, this is
   * "hikariConfigBean".
   */
  protected HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

  @Override
  protected Processor createProcessor() {
    return new JdbcTeeProcessor(
        schema,
        tableNameTemplate,
        customMappings,
        generatedColumnMappings, encloseTableName,
        rollbackOnError,
        useMultiRowOp,
        maxPrepStmtParameters,
        changeLogFormat,
        getHikariConfigBean(),
        defaultOperation,
        unsupportedAction
    );
  }
}
