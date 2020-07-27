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
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.PrimitiveFieldTypeChooserValues;

/**
 * Describe what types (all fields of given type) should be converted to what type.
 */
public class WholeTypeConverterConfig extends BaseConverterConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="INTEGER",
      label = "Source type",
      description = "Converts all fields of given type",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TYPE_CONVERSION"
  )
  @ValueChooserModel(PrimitiveFieldTypeChooserValues.class)
  public Field.Type sourceType;
}

