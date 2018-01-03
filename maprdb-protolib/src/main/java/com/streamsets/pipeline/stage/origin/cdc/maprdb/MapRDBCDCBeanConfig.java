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
package com.streamsets.pipeline.stage.origin.cdc.maprdb;

import com.streamsets.pipeline.api.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class MapRDBCDCBeanConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "streamsetsDataCollector",
      label = "Consumer Group",
      description = "Consumer Group",
      displayPosition = 20,
      group = "MAPR"
  )
  public String consumerGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MAP,
      defaultValue = "{ \"/stream:topic\": \"table\" }",
      label = "Topic List",
      description = "List of MapR Streams topics to consume. The Streams CDC topics typically follow the pattern /<stream_name>:<topic_name>. Table name is required for each topic.",
      displayPosition = 30,
      group = "MAPR"
  )
  public Map<String, String> topicTableList = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of threads to allocate for consuming all topics",
      displayPosition = 50,
      group = "MAPR"
  )
  public int numberOfThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Match Size (records)",
      description = "Maximum number of records per batch",
      displayPosition = 60,
      group = "MAPR"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a partial or empty batch",
      displayPosition = 70,
      group = "MAPR"
  )
  public int batchWaitTime;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "MapR Streams Configuration",
      description = "Additional MapR Streams properties to pass to the underlying Kafka consumer",
      displayPosition = 80,
      group = "MAPR"
  )
  public Map<String, String> streamsOptions;
}
