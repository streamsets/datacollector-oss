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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.List;

public class FieldValueConditionalReplacerConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields to Replace",
      description = "Specify names of fields ",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel
  public List<String> fieldNames;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LESS_THAN",
      label = "Operator",
      description = "Select comparison operator < = > ",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(OperatorChooser.class)
  public String operator;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Comparison Value",
      description = "Literal value to compare against",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String comparisonValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Replacement Value",
      description = "Replacement Value",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String replacementValue;

}
