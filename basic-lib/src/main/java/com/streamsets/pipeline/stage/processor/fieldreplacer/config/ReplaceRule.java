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
package com.streamsets.pipeline.stage.processor.fieldreplacer.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

public class ReplaceRule {
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Fields",
    description = "Field(s) that should be replaced.",
    group = "REPLACE",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {FieldEL.class},
    displayPosition = 10
  )
  @FieldSelectorModel(singleValued = true)
  public String fields;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Set to Null",
    description = "Whether the field should be set to NULL or not.",
    group = "REPLACE",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean setToNull;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "New value",
    description = "Replacement value for the given field(s).",
    group = "REPLACE",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class, FieldEL.class, TimeNowEL.class},
    dependencies = {
      @Dependency(configName = "setToNull", triggeredByValues = "false")
    },
    displayPosition = 30,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String replacement;
}
