/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.CommonError;
import org.apache.commons.el.ExpressionEvaluatorImpl;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ELEvaluator extends ELEval {

  private final String configName;
  private final Map<String, Object> constants;
  private final Map<String, Method> functions;
  private final FunctionMapperImpl functionMapper;
  private final List<ElFunctionDefinition> elFunctionDefinitions;
  private final List<ElConstantDefinition> elConstantDefinitions;

  // ExpressionEvaluatorImpl can be used as a singleton
  private static final ExpressionEvaluatorImpl EVALUATOR = new ExpressionEvaluatorImpl();

  public ELEvaluator(String configName, Class<?>... elFuncConstDefClasses) {
    this.configName = configName;
    constants = new HashMap<>();
    functions = new HashMap<>();
    elFunctionDefinitions = new ArrayList<>();
    elConstantDefinitions = new ArrayList<>();
    populateConstantsAndFunctions(elFuncConstDefClasses);
    this.functionMapper = new FunctionMapperImpl();
  }

  public ELEvaluator() {
    this.configName = null;
    this.constants = new HashMap<>();
    this.functions = new HashMap<>();
    this.functionMapper = new FunctionMapperImpl();
    elFunctionDefinitions = new ArrayList<>();
    elConstantDefinitions = new ArrayList<>();
  }

  private void populateConstantsAndFunctions(Class<?>... elFuncConstDefClasses) {
    for(Class<?> klass : elFuncConstDefClasses) {
      for(Method m : klass.getMethods()) {
        ElFunction elFunctionAnnot = m.getAnnotation(ElFunction.class);
        if(elFunctionAnnot != null) {
          String functionName = null;
          if(elFunctionAnnot.prefix().isEmpty()) {
            functionName = elFunctionAnnot.name();
          } else {
            functionName = elFunctionAnnot.prefix() + ":" + elFunctionAnnot.name();
          }
          functions.put(functionName, m);
          Annotation[][] parameterAnnotations = m.getParameterAnnotations();
          Class<?>[] parameterTypes = m.getParameterTypes();
          List<ElFunctionArgumentDefinition> elFunctionArgumentDefinitions = new ArrayList<>(
            parameterTypes.length);
          for(int i = 0; i < parameterTypes.length; i ++) {
            Annotation annotation = parameterAnnotations[i][0];
            elFunctionArgumentDefinitions.add(new ElFunctionArgumentDefinition(((ElParam)annotation).value(),
              parameterTypes[i].getSimpleName()));
          }
          elFunctionDefinitions.add(new ElFunctionDefinition(elFunctionAnnot.name(), elFunctionAnnot.description(),
            elFunctionAnnot.prefix(), m.getReturnType().getSimpleName(), elFunctionArgumentDefinitions));
        }
      }
      for(Field f : klass.getFields()) {
        ElConstant elConstant = f.getAnnotation(ElConstant.class);
        if(elConstant != null) {
          try {
            constants.put(elConstant.name(), f.get(null));
            elConstantDefinitions.add(new ElConstantDefinition(elConstant.name(), elConstant.description(),
              f.getType().getSimpleName()));
          } catch (IllegalAccessException e) {
            //FIXME: throw exception
          }
        }
      }
    }
  }

  @Override
  public String getConfigName() {
    return configName;
  }

  public ELEval.Variables createVariables() {
    ELEval.Variables variables = new ELEvaluator.Variables();
    if (constants != null) {
      for (Map.Entry<String, ?> entry : constants.entrySet()) {
        variables.addVariable(entry.getKey(), entry.getValue());
      }
    }
    return variables;
  }

  @Override
  public void parseEL(String el) throws ELEvalException {
    try {
      EVALUATOR.parseExpressionString(el);
    } catch (ELException e) {
      throw new ELEvalException(CommonError.CMN_0105, el, e.getMessage(), e);
    }
  }

  @Override
  public <T> T evaluate (final ELEval.Variables vars, String expression, Class<T> returnType) throws ELEvalException {

    VariableResolver variableResolver = new VariableResolver() {

      @Override
      public Object resolveVariable(String name) throws ELException {
        Object value = constants.get(name);
        if (value == null) {
          if (!vars.hasVariable(name)) {
            throw new ELException(Utils.format("Variable '{}' cannot be resolved", name));
          }
          value = vars.getVariable(name);
        }
        return value;
      }
    };
    try {
      return (T) EVALUATOR.evaluate(expression, returnType, variableResolver, functionMapper);
    } catch (ELException e) {
      throw new ELEvalException(CommonError.CMN_0104, expression, e.getMessage(), e);
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

  public static class Variables implements ELEval.Variables {

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

  public List<ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public List<ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }
}
