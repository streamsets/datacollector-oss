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
package com.streamsets.datacollector.el;

import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ELVariables implements ELVars {

  private final Map<String, Object> constants;
  private final Map<String, Object> variables;
  private final Map<String, Object> contextVariables;

  @SuppressWarnings("unchecked")
  public ELVariables() {
    this(Collections.EMPTY_MAP);
  }

  public ELVariables(Map<String, Object> constants) {
    this.constants = new HashMap<>(constants);
    variables = new HashMap<>();
    contextVariables = new HashMap<>();
  }

  private final void checkVariableName(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(TextUtils.isValidName(name), Utils.formatL("Invalid name '{}', must be '{}'",
      name, TextUtils.VALID_NAME));
  }

  @Override
  public Object getConstant(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(TextUtils.isValidName(name), Utils.formatL("Invalid name '{}', must be '{}'",
                                                                   name, TextUtils.VALID_NAME));
    return constants.get(name);
  }

  @Override
  public void addVariable(String name, Object value) {
    checkVariableName(name);
    variables.put(name, value);
  }

  @Override
  public void addContextVariable(String name, Object value) {
    checkVariableName(name);
    contextVariables.put(name, value);
  }

  @Override
  public Object getVariable(String name) {
    checkVariableName(name);
    return variables.get(name);
  }

  @Override
  public boolean hasVariable(String name) {
    checkVariableName(name);
    return variables.containsKey(name);
  }

  @Override
  public boolean hasContextVariable(String name) {
    checkVariableName(name);
    return contextVariables.containsKey(name);
  }

  @Override
  public Object getContextVariable(String name) {
    checkVariableName(name);
    return contextVariables.get(name);
  }
}
