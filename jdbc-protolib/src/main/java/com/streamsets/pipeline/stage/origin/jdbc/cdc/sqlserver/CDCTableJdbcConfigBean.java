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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeActionChooserValues;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategyChooserValues;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderStrategy;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.TableOrderStrategyChooserValues;

import java.util.List;

public class CDCTableJdbcConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Table Configs",
      displayPosition  = 20,
      group = "TABLE"
  )
  @ListBeanModel
  public List<CDCTableConfigBean> tableConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of parallel threads that read data",
      displayPosition = 80,
      group = "JDBC",
      min = 1
  )
  public int numberOfThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Direct Table Query",
      displayPosition = 82,
      group = "JDBC",
      defaultValue = "false"
  )
  public boolean useTable = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SWITCH_TABLES",
      label = "Per Batch Strategy",
      description = "Determines the strategy for each batch to generate records from.",
      displayPosition = 90,
      group = "JDBC"
  )
  @ValueChooserModel(BatchTableStrategyChooserValues.class)
  public BatchTableStrategy batchTableStrategy;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Batches from Result Set",
      description = "Determines the number of batches that can be generated from the fetched " +
          "result set after which result set is closed. Leave -1 to keep the result set open as long as possible",
      min = -1,
      max = Integer.MAX_VALUE,
      displayPosition = 142,
      group = "JDBC",
      dependsOn = "batchTableStrategy",
      triggeredByValue = "SWITCH_TABLES"
  )
  public int numberOfBatchesFromRs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Result Set Cache Size",
      description = "Determines how many open statements/result sets can be cached." +
          " Leave -1 to Opt Out and have one statement open per table.",
      displayPosition = 143,
      group = "JDBC",
      dependsOn = "batchTableStrategy",
      //For Process all rows we will need a cache with size 1, user does not have to configure it.
      triggeredByValue = "SWITCH_TABLES"
  )
  public int resultCacheSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Initial Table Order Strategy",
      description = "Determines the strategy for initial table ordering",
      displayPosition = 210,
      group = "ADVANCED"
  )
  @ValueChooserModel(TableOrderStrategyChooserValues.class)
  public TableOrderStrategy tableOrderStrategy;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Fetch Size",
      description = "Fetch Size for the JDBC Statement. Should not be 0",
      displayPosition = 220,
      group = "JDBC"
  )
  public int fetchSize;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Reconnect for each Query",
      description = "Reconnect to DB Server on each query",
      displayPosition = 230,
      group = "ADVANCED"
  )
  public boolean isReconnect;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "On Unknown Type",
      description = "Action that should be performed when an unknown type is detected in the result set.",
      defaultValue = "STOP_PIPELINE",
      displayPosition = 240,
      group = "ADVANCED"
  )
  @ValueChooserModel(UnknownTypeActionChooserValues.class)
  public UnknownTypeAction unknownTypeAction = UnknownTypeAction.STOP_PIPELINE;

  public static final String TABLE_JDBC_CONFIG_BEAN_PREFIX = "tableJdbcConfigBean.";
  public static final String TABLE_CONFIG = TABLE_JDBC_CONFIG_BEAN_PREFIX + "tableConfigs";

  public List<Stage.ConfigIssue> validateConfigs(PushSource.Context context, List<Stage.ConfigIssue> issues) {
    if (tableConfigs.isEmpty()) {
      issues.add(context.createConfigIssue(com.streamsets.pipeline.stage.origin.jdbc.table.Groups.TABLE.name(), TABLE_CONFIG, JdbcErrors.JDBC_66));
    }
    if (batchTableStrategy == BatchTableStrategy.SWITCH_TABLES && numberOfBatchesFromRs == 0) {
      issues.add(
          context.createConfigIssue(
              com.streamsets.pipeline.stage.origin.jdbc.table.Groups.JDBC.name(),
              TABLE_JDBC_CONFIG_BEAN_PREFIX +"numberOfBatchesFromRs",
              JdbcErrors.JDBC_76
          )
      );
    }
    return issues;
  }
}
