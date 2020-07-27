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
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

public class ExpressionProcessorConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/",
      label = "Output Field",
      description = "Use an existing field or enter a new field. Using an existing field overwrites the " +
                    "original value.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "EXPRESSIONS"

  )
  @FieldSelectorModel(singleValued = true)
  public String fieldToSet;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:value('/')}",
      label = "Field Expression",
      description = "Use the expression language to modify values in a field.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, ELSupport.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "EXPRESSIONS"
  )
  public String expression;

}
