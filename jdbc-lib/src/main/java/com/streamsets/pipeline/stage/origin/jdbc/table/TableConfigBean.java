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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TableConfigBean {
  public static final String DEFAULT_PARTITION_SIZE = "1000000";
  public static final int DEFAULT_MAX_NUM_ACTIVE_PARTITIONS = -1;

  public static final String PARTITIONING_MODE_FIELD = "partitioningMode";
  public static final String MAX_NUM_ACTIVE_PARTITIONS_FIELD = "maxNumActivePartitions";
  public static final String PARTITION_SIZE_FIELD = "partitionSize";

  public static final String PARTITIONING_MODE_DEFAULT_VALUE_STR = "DISABLED";
  public static final PartitioningMode PARTITIONING_MODE_DEFAULT_VALUE = PartitioningMode.valueOf(
      PARTITIONING_MODE_DEFAULT_VALUE_STR
  );

  public static final String ENABLE_NON_INCREMENTAL_FIELD = "enableNonIncremental";
  public static final boolean ENABLE_NON_INCREMENTAL_DEFAULT_VALUE = false;

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
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Non-Incremental Load",
      description = "Use non-incremental loading for any tables that do not have suitable keys or offset column" +
          " overrides defined.  Progress within the table will not be tracked.",
      displayPosition = 75,
      defaultValue = "" + ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
      group = "TABLE"
  )
  public boolean enableNonIncremental = ENABLE_NON_INCREMENTAL_DEFAULT_VALUE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multithreaded Partition Processing Mode",
      description = "Multithreaded processing of partitions mode. Required (validation error if not possible), Best" +
          " effort (use if possible, but don't fail validation if not), or disabled (no partitioning).",
      displayPosition = 80,
      defaultValue = PARTITIONING_MODE_DEFAULT_VALUE_STR,
      group = "TABLE"
  )
  @ValueChooserModel(PartitioningModeChooserValues.class)
  public PartitioningMode partitioningMode = PARTITIONING_MODE_DEFAULT_VALUE;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Partition Size",
      description = "Controls the size of partitions.  This value represents the range of values that will be covered" +
          " by a single partition.",
      displayPosition = 90,
      defaultValue = DEFAULT_PARTITION_SIZE,
      group = "TABLE",
      dependsOn = "partitioningMode",
      triggeredByValue = {"BEST_EFFORT", "REQUIRED"}
  )
  public String partitionSize = DEFAULT_PARTITION_SIZE;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Max Partitions",
      description = "The maximum number of partitions that can be processed at once. Includes active partitions with" +
          " rows left to read and completed partitions before they are pruned.",
      displayPosition = 100,
      defaultValue = "" + DEFAULT_MAX_NUM_ACTIVE_PARTITIONS,
      group = "TABLE",
      dependsOn = "partitioningMode",
      triggeredByValue = {"BEST_EFFORT", "REQUIRED"},
      min = -1
  )
  public int maxNumActivePartitions = DEFAULT_MAX_NUM_ACTIVE_PARTITIONS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Offset Column Conditions",
      description = "Additional conditions to apply for the offset column when a query is issued." +
          " These conditions will be a logical AND with last offset value filter already applied by default.",
      displayPosition = 200,
      group = "TABLE",
      elDefs = {OffsetColumnEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String extraOffsetColumnConditions;
}
