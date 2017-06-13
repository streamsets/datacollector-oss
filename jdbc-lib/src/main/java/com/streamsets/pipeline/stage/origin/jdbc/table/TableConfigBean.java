/**
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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TableConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Schema",
      description = "Schema Name",
      displayPosition = 20,
      group = "TABLE"
  )
  public String schema;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Table Name Pattern",
      description = "Pattern of the table names to read. Use a SQL like syntax.",
      displayPosition = 30,
      defaultValue = "%",
      group = "TABLE"
  )
  public String tablePattern;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Table Exclusion Pattern",
      description = "Pattern of the table names to exclude from being read. Use a Java regex syntax." +
          " Leave empty if no exclusion needed.",
      displayPosition = 40,
      group = "TABLE"
  )
  public String tableExclusionPattern;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Override Offset Columns",
      description = "Overrides the primary key(s) as the offset column(s).",
      displayPosition = 50,
      defaultValue = "false",
      group = "TABLE"
  )
  public boolean overrideDefaultOffsetColumns;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Offset Columns",
      description = "Specify offset column(s) to override default offset column(s)",
      displayPosition  = 60,
      group = "TABLE",
      dependsOn = "overrideDefaultOffsetColumns",
      triggeredByValue = "true"
  )
  @ListBeanModel
  public List<String> offsetColumns = new ArrayList<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Initial Offset",
      description = "Configure Initial Offset for each Offset Column.",
      displayPosition = 70,
      group = "TABLE",
      elDefs = {TimeNowEL.class, TimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public Map<String, String> offsetColumnToInitialOffsetValue = new LinkedHashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Offset Column Conditions",
      description = "Additional conditions to apply for the offset column when a query is issued." +
          " These conditions will be a logical AND with last offset value filter already applied by default.",
      displayPosition = 80,
      group = "TABLE",
      elDefs = {OffsetColumnEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String extraOffsetColumnConditions;
}
