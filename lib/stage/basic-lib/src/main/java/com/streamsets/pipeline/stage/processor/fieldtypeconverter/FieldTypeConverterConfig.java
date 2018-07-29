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
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;

/**
 * Describe what fields (by name) should be converted to what type.
 */
public class FieldTypeConverterConfig extends BaseConverterConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Convert",
      description = "You can convert multiple fields to the same type",
      displayPosition = 10,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, FieldEL.class},
      group = "TYPE_CONVERSION"
  )
  @FieldSelectorModel
  public List<String> fields;
}
