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
package com.streamsets.pipeline.stage.destination.redis;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;

public class RedisTargetConfig {

  public static final String REDIS_TARGET_CONFIG_PREFIX = "RedisTargetConfig.";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "URI",
      description = "Use format redis://[:password@]host:port[/[database]]",
      displayPosition = 10,
      group = "REDIS"
  )
  public String uri = "redis://:password@localhost:6379/0";

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Connection Timeout (sec)",
      defaultValue = "60",
      required = true,
      min = 1,
      displayPosition = 20,
      group = "REDIS"
  )
  public int connectionTimeout = 60;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Retry Attempts",
      defaultValue = "0",
      required = true,
      min = 0,
      displayPosition = 21,
      group = "REDIS"
  )
  public int maxRetries = 0;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Max Batch Wait Time",
      defaultValue = "${5 * SECONDS}",
      required = true,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 22,
      group = "REDIS"
  )
  public long maxBatchWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      description = "Whether to write the data in batches as key-value pairs or to publish the data as messages",
      defaultValue = "BATCH",
      displayPosition = 30,
      group = "REDIS"
  )
  @ValueChooserModel(ModeTypeChooserValues.class)
  public ModeType mode = ModeType.BATCH;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields",
      description = "Key names, their values and storage type",
      displayPosition = 40,
      group = "REDIS",
      dependsOn = "mode",
      triggeredByValue = {"BATCH"}
  )
  @ListBeanModel
  public List<RedisFieldMappingConfig> redisFieldMapping;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      defaultValue = "JSON",
      displayPosition = 1,
      group = "DATA_FORMAT",
      dependsOn = "mode",
      triggeredByValue = {"PUBLISH"}
  )
  @ValueChooserModel(ProducerDataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      defaultValue = "[]",
      label = "Channel",
      description = "Channel to publish the messages to",
      displayPosition = 50,
      group = "REDIS",
      dependsOn = "mode",
      triggeredByValue = {"PUBLISH"}
  )
  public List<String> channel;

  @ConfigDefBean(groups = "REDIS")
  public DataGeneratorFormatConfig dataFormatConfig = new DataGeneratorFormatConfig();

}
