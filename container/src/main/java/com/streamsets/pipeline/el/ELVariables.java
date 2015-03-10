/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class ELVariables implements ELVars {

  private final Map<String, Object> variables;

  public ELVariables() {
    variables = new HashMap<>();
  }

  public ELVariables(Map<String, Object> variables, Map<String, Object> contextVariables) {
    this();
    if (variables != null) {
      for (Map.Entry<String, Object> entry : variables.entrySet()) {
        addVariable(entry.getKey(), entry.getValue());
      }
    }
    if (contextVariables != null) {
      for (Map.Entry<String, Object> entry : contextVariables.entrySet()) {
        addContextVariable(entry.getKey(), entry.getValue());
      }
    }
  }

  private static final void checkVariableName(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(TextUtils.isValidName(name), Utils.formatL("Invalid name '{}', must be '{}'",
      name, TextUtils.VALID_NAME));
  }

  public void addVariable(String name, Object value) {
    checkVariableName(name);
    variables.put(name, value);
  }

  public void addContextVariable(String name, Object value) {
    checkVariableName(name);
    variables.put(":" + name, value);
  }

  public Object getVariable(String name) {
    checkVariableName(name);
    return variables.get(name);
  }

  public boolean hasVariable(String name) {
    checkVariableName(name);
    return variables.containsKey(name);
  }

  public boolean hasContextVariable(String name) {
    checkVariableName(name);
    return variables.containsKey(":" + name);
  }

  public Object getContextVariable(String name) {
    checkVariableName(name);
    return variables.get(":" + name);
  }
}
