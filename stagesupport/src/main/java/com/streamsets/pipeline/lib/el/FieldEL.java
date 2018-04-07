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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

public class FieldEL {
  public static final String FIELD_EL_PREFIX = "f";

  private static final String FIELD_PATH_CONTEXT_VAR = "fieldPath";
  private static final String FIELD_CONTEXT_VAR = "field";

  public static String getFieldPathInContext() {
    return (String) ELEval.getVariablesInScope().getContextVariable(FIELD_PATH_CONTEXT_VAR);
  }

  public static Field getFieldInContext() {
    return (Field) ELEval.getVariablesInScope().getContextVariable(FIELD_CONTEXT_VAR);
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "type",
      description = "Returns the type of the field in context")
  public static Field.Type getType() {
    return getFieldInContext().getType();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "path",
      description = "Returns the value of the path to the field in context, with respect to its containing record")
  @SuppressWarnings("unchecked")
  public static String getPath() {
    return getFieldPathInContext();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "value",
      description = "Returns the value of the field in context")
  @SuppressWarnings("unchecked")
  public static Object getValue() {
    return getFieldInContext().getValue();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "attribute",
      description = "Returns the specified attribute value of the field in context")
  @SuppressWarnings("unchecked")
  public static String getAttribute(@ElParam("attrName") String attrName) {
    return getFieldInContext().getAttribute(attrName);
  }

  public static void setFieldInContext(ELVars variables, String fieldPath, Field field) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(FIELD_PATH_CONTEXT_VAR, fieldPath);
    variables.addContextVariable(FIELD_CONTEXT_VAR, field);
  }
}
