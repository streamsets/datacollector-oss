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
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.concurrent.TimeUnit;

public class CacheConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Local Caching",
      description = "Select to enable caching of lookups. This improves performance, but should only be used when values rarely change",
      displayPosition = 30,
      group = "#0"
  )
  public boolean enabled = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Entries to Cache",
      min = -1,
      defaultValue = "-1",
      description = "Maximum number of key-value pairs to cache. If exceeded, oldest values are evicted to make room. Default value is -1 which is unlimited",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 40,
      group = "#0"
  )
  public long maxSize = -1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Eviction Policy Type",
      description = "Policy type used to evict key-value pairs from the local cache. " +
          "Select whether to reset the expiration time after the last write or after the last access of the value.",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 50,
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
      displayPosition = 60,
      group = "#0"
  )
  public long expirationTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Time Unit",
      defaultValue = "SECONDS",
      dependsOn = "enabled",
      triggeredByValue = "true",
      displayPosition = 70,
      group = "#0"
  )
  @ValueChooserModel(TimeUnitChooserValues.class)
  public TimeUnit timeUnit;
}
