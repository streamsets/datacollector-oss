/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.couchbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class EnvConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Property Name",
      description = "Name of the CouchbaseEnvironment property to set",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ENVIRONMENT"
  )
  public String name;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Value",
      description = "CouchbaseEnvironment property value",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ENVIRONMENT"
  )
  public String value;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Value Type",
      description="Data type of the property value",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ENVIRONMENT"
  )
  @ValueChooserModel(EnvDataTypeChooserValues.class)
  public EnvDataType type;

}
