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
package com.streamsets.pipeline.stage.lib.hive;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

public class FieldPathEL {
  private static final String FIELD_PATH_CONTEXT_PREFIX = "field";
  private static final String FIELD_PATH_CONTEXT_VAR = "field";

  @ElFunction(
      prefix = FIELD_PATH_CONTEXT_PREFIX,
      name = "field",
      description = "Returns the fieldPath currently being processed in context variable")
  public static String getFieldName() {
    return getFieldInContext();
  }
  public static void setFieldInContext(ELVars variables, String fieldPath) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(FIELD_PATH_CONTEXT_VAR, fieldPath);
  }
  public static String getFieldInContext() {
    return (String) ELEval.getVariablesInScope().getContextVariable(FIELD_PATH_CONTEXT_VAR);
  }
}
