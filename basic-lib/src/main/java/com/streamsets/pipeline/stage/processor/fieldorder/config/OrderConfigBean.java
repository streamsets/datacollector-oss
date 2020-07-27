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
package com.streamsets.pipeline.stage.processor.fieldorder.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.Collections;
import java.util.List;

public class OrderConfigBean {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "",
    label = "Fields to Order",
    description = "List of fields in the desired order",
    group = "ORDER",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "LIST_MAP",
    label = "Output Type",
    description = "Type of the output parent field",
    group = "ORDER",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(OutputTypeChooserValues.class)
  public OutputType outputType = OutputType.LIST_MAP;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="TO_ERROR",
    label = "Missing Fields",
    description = "Action to perform if the record doesn't have all the fields specified in the order list",
    group = "ORDER",
    displayPosition = 30,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(MissingFieldActionChooserValues.class)
  public MissingFieldAction missingFieldAction = MissingFieldAction.TO_ERROR;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Default Value",
    description = "Default value to be inserted for missing fields",
    dependencies = {
      @Dependency(configName = "missingFieldAction", triggeredByValues = "USE_DEFAULT")
    },
    group = "ORDER",
    displayPosition = 40,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String defaultValue;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Default Type",
    description = "Data type of the default value",
    dependencies = {
      @Dependency(configName = "missingFieldAction", triggeredByValues = "USE_DEFAULT")
    },
    group = "ORDER",
    displayPosition = 50,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType dataType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="TO_ERROR",
    label = "Extra Fields",
    description = "Action to perform if the record has additional fields that weren't specified in the order list",
    group = "ORDER",
    displayPosition = 60,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(ExtraFieldActionValueChooser.class)
  public ExtraFieldAction extraFieldAction = ExtraFieldAction.TO_ERROR;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    defaultValue = "",
    label = "Discard Fields",
    description = "List of fields that should be discarded if they appear in the source record",
    dependencies = {
      @Dependency(configName = "extraFieldAction", triggeredByValues = "TO_ERROR")
    },
    group = "ORDER",
    displayPosition = 70,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel
  public List<String> discardFields = Collections.emptyList();

}
