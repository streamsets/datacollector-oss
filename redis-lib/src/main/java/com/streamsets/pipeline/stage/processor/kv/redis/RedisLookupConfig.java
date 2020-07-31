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
package com.streamsets.pipeline.stage.processor.kv.redis;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import com.streamsets.pipeline.stage.processor.kv.LookupModeChooserValues;

import java.util.ArrayList;
import java.util.List;

public class RedisLookupConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "URI",
      description = "Use format redis://[:password@]host:port[/[database]]",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REDIS"
  )
  public String uri = "redis://:password@localhost:6379/0";

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Connection Timeout (sec)",
      defaultValue = "60",
      required = true,
      min = 1,
      group = "REDIS",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int connectionTimeout = 60;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      description = "Whether to perform a bulk lookup of all keys in the batch, or perform individual lookups per key.",
      defaultValue = "BATCH",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(LookupModeChooserValues.class)
  public LookupMode mode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Lookup Parameters",
      displayPosition = 20,
      group = "LOOKUP",
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ListBeanModel
  public List<RedisLookupParameterConfig> lookups = new ArrayList<>();

  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cache = new CacheConfig();

}
