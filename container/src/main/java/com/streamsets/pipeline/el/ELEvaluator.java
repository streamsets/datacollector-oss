/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.el;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.util.TextUtils;
import org.apache.commons.el.ExpressionEvaluatorImpl;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class ELEvaluator {

  public static class Variables {
    private final Map<String, Object> variables;

    public Variables() {
      variables = new HashMap<>();
    }

    public Variables(Map<String, Object> variables, Map<String, Object> contextVariables) {
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
      Preconditions.checkNotNull(name);
      Preconditions.checkArgument(TextUtils.isValidName(name), Utils.format("Invalid variable name '{}', must be '{}'",
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

    public Object getContextVariable(String name) {
      checkVariableName(name);
      return variables.get(":" + name);
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

    @Override
    public String toString() {
      return Utils.format("ELEvaluator.Variables[{}]", variables);
    }

  }

  private static String getFunctionName(String functionNamespace,String functionName) {
    if (functionNamespace.length() > 0) {
      functionName = functionNamespace + ":" + functionName;
    }
    return functionName;
  }

  private class FunctionMapperImpl implements FunctionMapper {

    @Override
    public Method resolveFunction(String functionNamespace, String functionName) {
      return functions.get(ELEvaluator.getFunctionName(functionNamespace, functionName));
    }
  }

  // ExpressionEvaluatorImpl can be used as a singleton
  private static final ExpressionEvaluatorImpl EVALUATOR = new ExpressionEvaluatorImpl();;
  private final static ThreadLocal<Variables> VARIABLES_IN_SCOPE_TL = new ThreadLocal<>();

  private final Map<String, Object> constants;
  private final Map<String, Method> functions;
  private final FunctionMapperImpl functionMapper;


  public ELEvaluator() {
    constants = new HashMap<>();
    functions = new HashMap<>();
    functionMapper = new FunctionMapperImpl();
  }

  public void registerFunction(String functionNamespace, String functionName, Method method) {
    Preconditions.checkNotNull(functionNamespace, "functionNamespace");
    Preconditions.checkNotNull(functionName, "functionName");
    Preconditions.checkArgument(!functionName.isEmpty(), "functionName cannot be empty");
    if ((method.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) != (Modifier.PUBLIC | Modifier.STATIC)) {
      throw new IllegalArgumentException(Utils.format("Method '{}' must be public and static", method));
    }
    functions.put(getFunctionName(functionNamespace, functionName), method);
  }

  public void registerConstant(String constantName, Object value) {
    Preconditions.checkNotNull(constantName, "constantName cannot be null");
    Preconditions.checkNotNull(value, "value cannot be null");
    Preconditions.checkArgument(!constantName.isEmpty(), "constantName cannot be empty");
    constants.put(constantName, value);
  }

  public static Variables getVariablesInScope() {
    return VARIABLES_IN_SCOPE_TL.get();
  }

  public Object eval(Variables variables, String expression) throws ELException {
    return eval(variables, expression, Object.class);
  }

  // use invalid java identifiers as names of variables that shouldn't be accessible via the EL, but only via the
  // getVariablesInContext() method for functions.
  @SuppressWarnings("unchecked")
  public <T> T eval(final Variables variables, String expression, Class<T> expected) throws ELException {
    Preconditions.checkNotNull(variables, "variables");
    Preconditions.checkNotNull(expression, "expression");
    Preconditions.checkNotNull(expected, "expected");
    try {
      VARIABLES_IN_SCOPE_TL.set(variables);
      VariableResolver variableResolver = new VariableResolver() {

        @Override
        public Object resolveVariable(String name) throws ELException {
          Object value = constants.get(name);
          if (value == null) {
            if (!variables.hasVariable(name)) {
              throw new ELException(Utils.format("Variable '{}' cannot be resolved", name));
            }
            value = variables.getVariable(name);
          }
          return value;
        }
      };
      return (T) EVALUATOR.evaluate(expression, expected, variableResolver, functionMapper);
    } finally {
      VARIABLES_IN_SCOPE_TL.set(null);
    }
  }

}
