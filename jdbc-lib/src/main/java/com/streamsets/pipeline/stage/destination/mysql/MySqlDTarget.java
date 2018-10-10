/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mysql;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.ChangeLogFormatChooserValues;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationActionChooserValues;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "MemSQL Fast Loader",
    description = "Insert, update, delete, and load data to a MemSQL or MySQL destination.",
    upgrader = MySqlTargetUpgrader.class,
    icon = "memsql.svg",
    onlineHelpRefUrl ="index.html?contextID=task_gbs_n1f_kfb"
)
@ConfigGroups(value = Groups.class)
@HideConfigs(value = {
    "hikariConfigBean.readOnly",
    "hikariConfigBean.autoCommit",
})
public class MySqlDTarget extends DTarget {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Schema Name",
      displayPosition = 20,
      group = "JDBC"
  )
  public String schema;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "",
      label = "Table Name",
      description = "Table Names should contain only table names. Schema should be defined in the connection string or " +
          "schema configuration",
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
  public List<JdbcFieldColumnParamMapping> columnNames;


  @ConfigDefBean()
  public HikariPoolConfigBean hikariConfigBean;

  @Override
  protected Target createTarget() {
    return new MySqlTarget(
        schema,
        tableNameTemplate,
        columnNames,
        hikariConfigBean
    );
  }
}
