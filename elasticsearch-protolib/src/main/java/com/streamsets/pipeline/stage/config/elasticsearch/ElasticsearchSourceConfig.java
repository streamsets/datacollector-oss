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
package com.streamsets.pipeline.stage.config.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.OffsetEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection;

public class ElasticsearchSourceConfig extends ElasticsearchConfig {

  private static final String QUERY_CONFIG_NAME = "query";

  public static final String QUERY_CONFIG_PATH = QUERY_CONFIG_NAME;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Index",
      description = "Optional index to scope query",
      group = "ELASTIC_SEARCH",
      displayMode = ConfigDef.DisplayMode.BASIC,
      displayPosition = 50
  )
  public String index;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Mapping",
      description = "Optional type mapping to scope query",
      group = "ELASTIC_SEARCH",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String mapping;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Incremental Mode",
      description = "Fetches data incrementally based on the offset field until the pipeline is stopped",
      defaultValue = "false",
      group = "ELASTIC_SEARCH",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean isIncrementalMode = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Query Interval",
      description = "Interval at which to run the provided incremental query",
      defaultValue = "${1 * HOURS}",
      elDefs = TimeEL.class,
      dependsOn = "isIncrementalMode",
      triggeredByValue = "true",
      group = "ELASTIC_SEARCH",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public long queryInterval = 3600;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Offset Field",
      description = "Field name to use for tracking offset in the result.",
      defaultValue = "timestamp",
      dependsOn = "isIncrementalMode",
      triggeredByValue = "true",
      group = "ELASTIC_SEARCH",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String offsetField = "timestamp";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Initial Offset",
      description = "Initial value to use when Reset Origin is performed.",
      defaultValue = "now-1d/d",
      dependsOn = "isIncrementalMode",
      triggeredByValue = "true",
      group = "ELASTIC_SEARCH",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String initialOffset = "now-1d/d";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.JSON,
      label = "Query",
      defaultValue = "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}",
      description = "Elasticsearch query to run",
      elDefs = OffsetEL.class,
      group = "ELASTIC_SEARCH",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String query = "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Scroll Timeout",
      description = "Maximum amount of time to keep the search context alive",
      defaultValue = "1m",
      group = "ELASTIC_SEARCH",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public String cursorTimeout = "1m";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Delete Scroll on Pipeline Stop",
      description = "Whether to explicitly delete the scroll on pipeline stop, or let it expire",
      defaultValue = "true",
      group = "ELASTIC_SEARCH",
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean deleteCursor = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Batch Size",
      description = "Maximum results to fetch per request from Elasticsearch",
      defaultValue = "1000",
      group = "ELASTIC_SEARCH",
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int maxBatchSize = 1000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Slices",
      description = "Number of slices per scroll to process in parallel",
      min = 1,
      max = 200,
      group = "ELASTIC_SEARCH",
      displayPosition = 170,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int numSlices = 1;
}
