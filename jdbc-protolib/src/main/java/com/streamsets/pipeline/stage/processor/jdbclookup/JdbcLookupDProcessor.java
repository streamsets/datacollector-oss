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
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeActionChooserValues;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MissingValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;

import java.util.List;

@StageDef(
    version = 6,
    label = "JDBC Lookup",
    description = "Lookup values via JDBC to enrich records.",
    icon = "rdbms.png",
    upgrader = JdbcLookupProcessorUpgrader.class,
    upgraderDef = "upgrader/JdbcLookupDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_kbr_2cy_hw"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JdbcLookupDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "SQL Query",
      description = "SELECT <column>, ... FROM <table name> WHERE <column> <operator>  <expression>",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 20,
      group = "JDBC"
  )
  public String query;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Validate Column Mappings",
      description = "Requires that all columns listed in the Column Mappings configuration exist in the database",
      displayPosition = 29,
      group = "JDBC"
  )
  public boolean validateColumnMappings;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Column Mappings",
      defaultValue = "",
      description = "Mappings from column names to field names",
      displayPosition = 30,
      group = "JDBC"
  )
  @ListBeanModel
  public List<JdbcFieldColumnMapping> columnMappings;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values",
      defaultValue = "FIRST_ONLY",
      displayPosition = 35,
      group = "JDBC"
  )
  @ValueChooserModel(MultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Missing Values Behavior",
      description = "How to handle missing values when no default value is defined.",
      defaultValue = "PASS_RECORD_ON",
      displayPosition = 37,
      group = "JDBC"
  )
  @ValueChooserModel(MissingValuesBehaviorChooserValues.class)
  public MissingValuesBehavior missingValuesBehavior = MissingValuesBehavior.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Clob Size (Characters)",
      displayPosition = 40,
      group = "JDBC"
  )
  public int maxClobSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Blob Size (Bytes)",
      displayPosition = 50,
      group = "JDBC"
  )
  public int maxBlobSize;

  @ConfigDefBean()
  public JdbcHikariPoolConfigBean hikariConfigBean;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "On Unknown Type",
      description = "Action that should be performed when an unknown type is detected in the result set.",
      defaultValue = "STOP_PIPELINE",
      displayPosition = 230,
      group = "ADVANCED"
  )
  @ValueChooserModel(UnknownTypeActionChooserValues.class)
  public UnknownTypeAction unknownTypeAction = UnknownTypeAction.STOP_PIPELINE;

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

  @ConfigDefBean(groups = "JDBC")
  public CacheConfig cacheConfig = new CacheConfig();

  @Override
  protected Processor createProcessor() {
    return new JdbcLookupProcessor(
      query,
      validateColumnMappings,
      columnMappings,
      multipleValuesBehavior,
      missingValuesBehavior,
      unknownTypeAction,
      maxClobSize,
      maxBlobSize,
      getHikariConfigBean(),
      cacheConfig
    );
  }
}
