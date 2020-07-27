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
package com.streamsets.pipeline.stage.processor.schemagen.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class AvroDefaultConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "",
    label = "Avro Type",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(AvroTypeValueChooser.class)
  public AvroType avroType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Default Value",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String defaultValue;

  // Constructor for framework
  public AvroDefaultConfig() {
  }

  // Constructor for test
  public AvroDefaultConfig(AvroType type, String defaultValue) {
    this.avroType = type;
    this.defaultValue = defaultValue;
  }
}
