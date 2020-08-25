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
package com.streamsets.pipeline.stage.bigquery.origin;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.googlecloud.BigQueryCredentialsConfig;

public class BigQuerySourceConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "Query",
      displayMode = ConfigDef.DisplayMode.BASIC,
      displayPosition = 10,
      group = "BIGQUERY"
  )
  public String query = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Legacy SQL",
      description = "By default, standard SQL is used. When checked, legacy SQL is used.",
      defaultValue = "false",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "BIGQUERY"
  )
  public boolean useLegacySql = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use cached query results",
      defaultValue = "true",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "BIGQUERY"
  )
  public boolean useQueryCache = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${5 * MINUTES}",
      label = "Query Timeout (sec)",
      elDefs = TimeEL.class,
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "BIGQUERY",
      min = 1
  )
  public long timeout = 300;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (records)",
      description = "Max number of records per batch",
      displayPosition = 1000,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxBatchSize = 1000;

  @ConfigDefBean(groups = "CREDENTIALS")
  public BigQueryCredentialsConfig credentials = new BigQueryCredentialsConfig();
}
