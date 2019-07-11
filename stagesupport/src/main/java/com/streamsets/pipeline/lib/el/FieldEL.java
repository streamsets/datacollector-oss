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

import java.util.EnumSet;

public class FieldEL {
  public static final String FIELD_EL_PREFIX = "f";

  private static final String FIELD_PATH_CONTEXT_VAR = "fieldPath";
  private static final String FIELD_CONTEXT_VAR = "field";
  private static final String FIELD_NAME_VAR = "name";
  private static final String PARENT_FIELD_PATH_CONTEXT_VAR = "parentFieldPath";
  private static final String PARENT_FIELD_CONTEXT_VAR = "parentField";
  private static final String INDEX_WITHIN_PARENT_CONTEXT_VAR = "indexWithinParent";

  public static String getFieldPathInContext() {
    return (String) ELEval.getVariablesInScope().getContextVariable(FIELD_PATH_CONTEXT_VAR);
  }

  public static String getFieldNameInContext() {
    return (String) ELEval.getVariablesInScope().getContextVariable(FIELD_NAME_VAR);
  }

  public static Field getFieldInContext() {
    return (Field) ELEval.getVariablesInScope().getContextVariable(FIELD_CONTEXT_VAR);
  }

  public static String getParentFieldPathInContext() {
    return (String) ELEval.getVariablesInScope().getContextVariable(PARENT_FIELD_PATH_CONTEXT_VAR);
  }

  public static Field getParentFieldInContext() {
    return (Field) ELEval.getVariablesInScope().getContextVariable(PARENT_FIELD_CONTEXT_VAR);
  }

  public static int getIndexWithinParentInContext() {
    final Object val = ELEval.getVariablesInScope().getContextVariable(INDEX_WITHIN_PARENT_CONTEXT_VAR);
    return val == null ? -1 : (int) val;
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "type",
      description = "Returns the type of the field in context"
  )
  public static Field.Type getType() {
    return getFieldInContext().getType();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "path",
      description = "Returns the value of the path to the field in context, with respect to its containing record"
  )
  public static String getPath() {
    return getFieldPathInContext();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "name",
      description = "Returns the value of the field name (last portion of path) to the field in context, with respect" +
          " to its containing record"
  )
  public static String getName() {
    return getFieldNameInContext();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "value",
      description = "Returns the value of the field in context"
  )
  public static Object getValue() {
    return getFieldInContext().getValue();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "attribute",
      description = "Returns the specified attribute value of the field in context"
  )
  public static String getAttribute(@ElParam("attrName") String attrName) {
    return getFieldInContext().getAttribute(attrName);
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "index",
      description = "Returns the index of the current field within a parent LIST, or -1 if not applicable"
  )
  public static int getIndex() {
    return getIndexWithinParentInContext();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "parentPath",
      description = "Returns the path to the parent field, if available"
  )
  public static String getParentPath() {
    return getParentFieldPathInContext();
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "parent",
      description = "Returns the parent field, if available")
  public static Field getParentField() {
    return getParentFieldInContext();
  }

  private static Field getSiblingWithName(Field parentField, String siblingName) {
    if (parentField == null) {
      return null;
    }
    if (!EnumSet.of(Field.Type.MAP, Field.Type.LIST_MAP).contains(parentField.getType())) {
      return null;
    }
    return parentField.getValueAsMap().get(siblingName);
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "getSiblingWithName",
      description = "Returns the sibling field with the given name, if it exists"
  )
  public static Field getSiblingWithName(@ElParam("siblingName") String siblingName) {
    return getSiblingWithName(getParentFieldInContext(), siblingName);
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "hasSiblingWithName",
      description = "Returns true if a sibling field exists with the given name"
  )
  public static boolean hasSiblingWithName(@ElParam("siblingName") String siblingName) {
    return getSiblingWithName(getParentFieldInContext(), siblingName) != null;
  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "hasSiblingWithValue",
      description = "Returns true if a sibling field exists with the given name and value"
  )
  public static boolean hasSiblingWithValue(
      @ElParam("siblingName") String siblingName,
      @ElParam("value") Object value
  ) {
    final Field siblingWithName = getSiblingWithName(getParentFieldInContext(), siblingName);

    if (siblingWithName == null) {
      return false;
    }
    final Object siblingFieldValue = siblingWithName.getValue();
    return siblingFieldValue != null && siblingFieldValue.equals(value);
  }

  public static void setFieldInContext(
      ELVars variables,
      String fieldPath,
      String fieldName,
      Field field
  ) {
    setFieldInContext(variables, fieldPath, fieldName, field, null, null, -1);
  }

  public static void setFieldInContext(
      ELVars variables,
      String fieldPath,
      String fieldName,
      Field field,
      String parentFieldPath,
      Field parentField,
      int indexWithinParent
  ) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(FIELD_PATH_CONTEXT_VAR, fieldPath);
    variables.addContextVariable(FIELD_NAME_VAR, fieldName);
    variables.addContextVariable(FIELD_CONTEXT_VAR, field);
    variables.addContextVariable(PARENT_FIELD_PATH_CONTEXT_VAR, parentFieldPath);
    variables.addContextVariable(PARENT_FIELD_CONTEXT_VAR, parentField);
    variables.addContextVariable(INDEX_WITHIN_PARENT_CONTEXT_VAR, indexWithinParent);

  }

}
