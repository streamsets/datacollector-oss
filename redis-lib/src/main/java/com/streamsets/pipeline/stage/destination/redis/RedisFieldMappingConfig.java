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
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.redis.DataType;
import com.streamsets.pipeline.lib.redis.DataTypeChooserValues;

public class RedisFieldMappingConfig {
  /**
   * Parameter-less constructor required.
   */
  public RedisFieldMappingConfig() {
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/",
      label = "Key",
      description = "Field to use for the key",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String keyExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/",
      label = "Value",
      description = "Field to use for the value",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String valExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "STRING",
      label = "Data Type",
      description = "The data type for the value",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType dataType = DataType.STRING;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "",
      label = "TTL (sec)",
      description = "Set a timeout on key (sec)",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int ttl = -1;
}
