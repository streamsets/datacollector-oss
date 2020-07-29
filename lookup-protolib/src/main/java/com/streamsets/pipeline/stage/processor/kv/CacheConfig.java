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
package com.streamsets.pipeline.stage.processor.kv;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.concurrent.TimeUnit;

public class CacheConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Local Caching",
      description = "Select to enable caching of lookups. This improves performance, but should only be used when values rarely change",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean enabled = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Entries to Cache",
      min = -1,
      defaultValue = "-1",
      description = "Maximum number of values to cache. If exceeded, oldest values are evicted to make room. Default value is -1 which is unlimited",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public long maxSize = -1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Eviction Policy Type",
      description = "Policy type used to evict values from the local cache. " +
          "Select whether to reset the expiration time after the last write or after the last access of the value.",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(EvictionPolicyTypeChooserValues.class)
  public EvictionPolicyType evictionPolicyType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Expiration Time",
      min = 0,
      defaultValue = "1",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public long expirationTime = 1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Time Unit",
      defaultValue = "SECONDS",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(TimeUnitChooserValues.class)
  public TimeUnit timeUnit = TimeUnit.SECONDS;

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Retry on Missing Value",
      defaultValue = "false",
      description = "By default, the processor remembers that a look up for a given column failed and always returns" +
        " the default value. This is to avoid doing unnecessary look ups for known missing values. Select this option" +
        " if new values might be inserted and the processor should retry the request rather than returning the cached" +
        " default value.",
      displayPosition = 150,
      dependencies = @Dependency(configName = "enabled", triggeredByValues = "true"),
      group = "#0"
  )
  public boolean retryOnCacheMiss = false;
}
