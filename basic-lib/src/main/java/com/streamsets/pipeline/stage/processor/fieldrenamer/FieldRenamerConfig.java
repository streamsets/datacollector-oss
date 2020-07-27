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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class FieldRenamerConfig {

  /**
   * Parameter-less constructor required.
   */
  public FieldRenamerConfig() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Source Field Expression",
      description = "Existing fields to rename. You can use regular expressions to rename a set of fields.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RENAME"
  )
  public String fromFieldExpression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Target Field Expression",
      description = "New name for the field. You can use regular expressions to rename a set of fields.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "RENAME"
  )
  public String toFieldExpression;
}
