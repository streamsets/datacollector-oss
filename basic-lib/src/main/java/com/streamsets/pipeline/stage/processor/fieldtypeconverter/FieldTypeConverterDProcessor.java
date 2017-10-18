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

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.ArrayList;
import java.util.List;

@StageDef(
    version = 2,
    label = "Field Type Converter",
    description = "Converts the data type of a field(s)",
    icon = "converter.png",
    onlineHelpRefUrl = "index.html#Processors/FieldTypeConverter.html#task_g23_2tq_wq",
    upgrader = FieldTypeConverterProcessorUpgrader.class
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldTypeConverterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="BY_FIELD",
      label = "Conversion Method",
      description = "Select type of conversion that will be performed.",
      displayPosition = 5,
      group = "TYPE_CONVERSION"
  )
  @ValueChooserModel(ConvertByChooserValues.class)
  public ConvertBy convertBy;

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "",
      description = "Configures field by names that should be converted",
      displayPosition = 10,
      group = "TYPE_CONVERSION",
      dependsOn = "convertBy",
      triggeredByValue = "BY_FIELD"
  )
  @ListBeanModel
  public List<FieldTypeConverterConfig> fieldTypeConverterConfigs = new ArrayList<>();

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "",
      description = "Configure types that should be converted. All fields of given type in a record will be converted.",
      displayPosition = 10,
      group = "TYPE_CONVERSION",
      dependsOn = "convertBy",
      triggeredByValue = "BY_TYPE"
  )
  @ListBeanModel
  public List<WholeTypeConverterConfig> wholeTypeConverterConfigs = new ArrayList<>();


  @Override
  protected Processor createProcessor() {
    return new FieldTypeConverterProcessor(
      convertBy,
      fieldTypeConverterConfigs,
      wholeTypeConverterConfigs
        );
  }
}
