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
package com.streamsets.pipeline.stage.processor.fieldflattener;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.LinkedList;
import java.util.List;

public class FieldFlattenerConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="ENTIRE_RECORD",
    label = "Flatten",
    description = "Select what should be flattened in the record",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN"
  )
  @ValueChooserModel(FlattenTypeChooserValues.class)
  public FlattenType flattenType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Fields",
    dependsOn = "flattenType",
    description = "List of fields to be flattened",
    displayPosition = 15,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN",
    triggeredByValue = { "SPECIFIC_FIELDS" }
  )
  @FieldSelectorModel
  public List<String> fields = new LinkedList<>();

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Flatten in Place",
    defaultValue = "true",
    description = "When set, each field will be flattened in place.",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN",
    dependsOn = "flattenType",
    triggeredByValue = { "SPECIFIC_FIELDS" }
  )
  public boolean flattenInPlace = true;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Target Field",
    description = "Field (must be MAP or MAP_LIST) into which the fields should be flattened.",
    displayPosition = 25,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN",
    dependsOn = "flattenInPlace",
    triggeredByValue = { "false" }
  )
  @FieldSelectorModel(singleValued = true)
  public String flattenTargetField;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="TO_ERROR",
    label = "Collision Field Action",
    description = "Action that should be performed when he target field already have field with given name.",
    displayPosition = 30,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN",
    dependsOn = "flattenInPlace",
    triggeredByValue = { "false" }
  )
  @ValueChooserModel(CollisionFieldActionValueChooser.class)
  public CollisionFieldAction collisionFieldAction;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Remove Flattened Field",
    defaultValue = "true",
    description = "When set, flattened field will be removed after successful flattening.",
    displayPosition = 35,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN",
    dependsOn = "flattenInPlace",
    triggeredByValue = { "false" }
  )
  public boolean removeFlattenedField = true;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Name separator",
    defaultValue = ".",
    description = "Separator that is used when created merged field name from nested structures.",
    displayPosition = 35,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FLATTEN"
  )
  public String nameSeparator;

}
