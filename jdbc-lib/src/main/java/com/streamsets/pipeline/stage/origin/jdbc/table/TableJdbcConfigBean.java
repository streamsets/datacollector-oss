/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.Groups;

import java.util.List;

public class TableJdbcConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Tables",
      displayPosition  = 10,
      group = "TABLE"
  )
  @ListBeanModel
  public List<TableConfigBean> tableConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve time based expressions",
      displayPosition = 70,
      group = "JDBC"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SWITCH_TABLES",
      label = "Per Batch Strategy",
      description = "Determines the strategy for each batch to generate records from.",
      displayPosition = 80,
      group = "JDBC"
  )
  @ValueChooserModel(BatchTableStrategyChooserValues.class)
  public BatchTableStrategy batchTableStrategy;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Table Order Strategy",
      description = "Determines the strategy for table ordering",
      displayPosition = 180,
      group = "ADVANCED"
  )
  @ValueChooserModel(TableOrderStrategyChooserValues.class)
  public TableOrderStrategy tableOrderStrategy;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Configure Fetch Size",
      description = "Determines whether to configure fetch size for the JDBC Statement",
      displayPosition = 190,
      group = "ADVANCED"
  )
  public boolean configureFetchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "",
      label = "Fetch Size",
      description = "Fetch Size for the JDBC Statement. Should not be 0 and Should be less than or equal to batch size.",
      displayPosition = 200,
      group = "ADVANCED",
      dependsOn = "configureFetchSize",
      triggeredByValue = "true"
  )
  public int fetchSize;

  private static final String TABLE_JDBC_CONFIG_BEAN_PREFIX = "tableJdbcConfigBean.";
  public static final String TABLE_CONFIG = TABLE_JDBC_CONFIG_BEAN_PREFIX + "tableConfigs";
  private static final String FETCH_SIZE = TABLE_JDBC_CONFIG_BEAN_PREFIX + "fetchSize";

  public List<Stage.ConfigIssue> validateConfigs(Source.Context context, List<Stage.ConfigIssue> issues, CommonSourceConfigBean commonSourceConfigBean) {
    if (configureFetchSize && fetchSize > commonSourceConfigBean.maxBatchSize) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), FETCH_SIZE, JdbcErrors.JDBC_65, fetchSize));
    }
    if (tableConfigs.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), TABLE_CONFIG, JdbcErrors.JDBC_66));
    }
    return issues;
  }
}
