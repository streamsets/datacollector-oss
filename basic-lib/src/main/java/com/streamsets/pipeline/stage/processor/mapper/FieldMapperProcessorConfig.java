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
package com.streamsets.pipeline.stage.processor.mapper;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.processor.expression.ELSupport;
import com.streamsets.pipeline.stage.processor.http.HeaderOutputLocation;
import com.streamsets.pipeline.stage.processor.http.HeaderOutputLocationChooserValues;

public class FieldMapperProcessorConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Operate On",
      description = "Controls whether this mapper will operate on field paths, names, or values.",
      defaultValue = "FIELD_VALUES",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MAPPER"
  )
  @ValueChooserModel(OperateOnChooserValues.class)
  public OperateOn operateOn;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Conditional Expression",
      description = "An expression that controls whether the mapper should apply to any particular field in the input" +
          "record. Leave this blank to apply the mapping to all fields in the record (unconditionally).",
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, FieldEL.class, ELSupport.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "MAPPER"
  )
  public String conditionalExpression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${f:value()}",
      label = "Mapping Expression",
      description = "The expression that transforms the field path, name, or value.",
      displayPosition = 300,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, FieldEL.class, ELSupport.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "MAPPER"
  )
  public String mappingExpression;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${fields}",
      label = "Aggregation Expression",
      description = "An expression that controls how to aggregate the results, if multiple fields map to the same " +
          "path.  The results will be available in a variable called \"fields\".",
      displayPosition = 300,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, FieldEL.class, ELSupport.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "MAPPER",
      dependsOn = "operateOn",
      triggeredByValue = {"FIELD_PATHS"}
  )
  public String aggregationExpression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Structure Change Allowed",
      description = "Controls whether the mapping is allowed to change the structure of the record by adding new " +
          "fields, changing types, etc.",
      displayPosition = 400,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MAPPER",
      dependsOn = "operateOn",
      triggeredByValue = {"FIELD_PATHS", "FIELD_NAMES"}
  )
  public boolean structureChangeAllowed = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Append List Values",
      description = "Controls what happens if multiple fields map to an existing list field. If true, mapped fields " +
          "will be appended to this list. If false, the mapped fields will replace all existing values in the list.",
      displayPosition = 500,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MAPPER",
      dependsOn = "operateOn",
      triggeredByValue = {"FIELD_PATHS", "FIELD_NAMES"}
  )
  public boolean appendListValues = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Maintain Original Paths",
      description = "When a field is mapped to a new location (either name or path), it is removed unless this " +
          "property is set.",
      displayPosition = 600,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MAPPER",
      dependsOn = "operateOn",
      triggeredByValue = {"FIELD_PATHS", "FIELD_NAMES"}
  )
  public boolean maintainOriginalPaths = false;

}
