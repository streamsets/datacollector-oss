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
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeActionChooserValues;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategyChooserValues;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderStrategyChooserValues;

import java.util.List;

public class TableJdbcConfigBean {
  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Table Configs",
      displayPosition  = 20,
      group = "TABLE"
  )
  @ListBeanModel
  public List<TableConfigBeanImpl> tableConfigs;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve time based expressions",
      displayPosition = 200,
      group = "JDBC"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Quote Character",
      description = "Determines the quote character to be used on table / schema / column names during query.",
      displayPosition = 205,
      group = "JDBC"
  )
  @ValueChooserModel(QuoteCharChooserValues.class)
  public QuoteChar quoteChar;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Create JDBC Header Attributes",
      description = "Generates record header attributes that provide additional details about source data, such as the original data type or source table name.",
      defaultValue = "true",
      displayPosition = 220,
      group = "ADVANCED"
  )
  public boolean createJDBCHeaders = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Fetch Size",
      description = "Fetch Size for the JDBC Statement. Should not be 0",
      displayPosition = 220,
      group = "JDBC"
  )
  public int fetchSize;

  public static final String TABLE_JDBC_CONFIG_BEAN_PREFIX = "tableJdbcConfigBean.";
  public static final String TABLE_CONFIG = TABLE_JDBC_CONFIG_BEAN_PREFIX + "tableConfigs";
  public static final String BATCHES_FROM_THE_RESULT_SET = "numberOfBatchesFromRs";
  public static final String NUMBER_OF_THREADS = "numberOfThreads";
  public static final String QUOTE_CHAR = "quoteChar";

  public List<Stage.ConfigIssue> validateConfigs(PushSource.Context context, DatabaseVendor vendor, List<Stage.ConfigIssue> issues) {
    if (getTableConfigs().isEmpty()) {
      issues.add(context.createConfigIssue(Groups.TABLE.name(), TABLE_CONFIG, JdbcErrors.JDBC_66));
    }
    if (batchTableStrategy == BatchTableStrategy.SWITCH_TABLES && numberOfBatchesFromRs == 0) {
      issues.add(
          context.createConfigIssue(
              Groups.JDBC.name(),
              TABLE_JDBC_CONFIG_BEAN_PREFIX +"numberOfBatchesFromRs",
              JdbcErrors.JDBC_76
          )
      );
    }

    if (!vendor.validQuoteCharacter(quoteChar.getLabel())) {
      issues.add(
          context.createConfigIssue(
              Groups.JDBC.name(),
              TABLE_JDBC_CONFIG_BEAN_PREFIX + "quoteChar",
              JdbcErrors.JDBC_414,
              quoteChar.getLabel()
          )
      );
    }

    return issues;
  }

  public List<? extends TableConfigBean> getTableConfigs() {
    return tableConfigs;
  }
}
