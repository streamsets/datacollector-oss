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
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;

public class NullReplacerConditionalConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields",
      group = "REPLACE",
      description = "Specify fields to null",
      displayPosition = 10)
  @FieldSelectorModel
  public List<String> fieldsToNull;

  @ConfigDef(
      //NOT Required   - empty means no condition and assume true.
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Condition",
      group = "REPLACE",
      description = "Condition for replacing the fields with null. Leave empty to replace all the configured fields with null",
      displayPosition = 20,
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String condition;
}
