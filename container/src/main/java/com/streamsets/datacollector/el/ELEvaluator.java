/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.CommonError;
import org.apache.commons.el.ExpressionEvaluatorImpl;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
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

  public ELEvaluator(String configName, Map<String, Object> constants, Class<?>... elFuncConstDefClasses) {
    this.configName = configName;
    this.constants = new HashMap<>(constants);
    functions = new HashMap<>();
    elFunctionDefinitions = new ArrayList<>();
    elConstantDefinitions = new ArrayList<>();
    populateConstantsAndFunctions(elFuncConstDefClasses);
    this.functionMapper = new FunctionMapperImpl();
  }

  public ELEvaluator(String configName, Class<?>... elFuncConstDefClasses) {
    this(configName, new HashMap<String, Object>(), elFuncConstDefClasses);
  }

  private void populateConstantsAndFunctions(Class<?>... elFuncConstDefClasses) {
    if(elFuncConstDefClasses != null) {
      for (ElFunctionDefinition function : ELDefinitionExtractor.get().extractFunctions(elFuncConstDefClasses, "")) {
        elFunctionDefinitions.add(function);
        functions.put(function.getName(), function.getMethod());
      }
      for (ElConstantDefinition constant : ELDefinitionExtractor.get().extractConstants(elFuncConstDefClasses, "")) {
        elConstantDefinitions.add(constant);
        constants.put(constant.getName(), constant.getValue());
      }
    }
  }

  @Override
  public String getConfigName() {
    return configName;
  }

  public ELVars createVariables() {
    return new ELVariables(constants);
  }

  public static void parseEL(String el) throws ELEvalException {
    try {
      EVALUATOR.parseExpressionString(el);
    } catch (ELException e) {
      throw new ELEvalException(CommonError.CMN_0105, el, e.toString(), e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T evaluate (final ELVars vars, String expression, Class<T> returnType) throws ELEvalException {

    VariableResolver variableResolver = new VariableResolver() {

      @Override
      public Object resolveVariable(String name) throws ELException {
        Object value = constants.get(name);
        if (!vars.hasVariable(name)) {
          if (value == null) {
            throw new ELException(Utils.format("Variable '{}' cannot be resolved", name));
          }
        } else {
          value = vars.getVariable(name);
        }
        return value;
      }
    };
    try {
      return (T) EVALUATOR.evaluate(expression, returnType, variableResolver, functionMapper);
    } catch (ELException e) {
      Throwable t = e;
      if(e.getRootCause() != null) {
        t = e.getRootCause();
      }
      throw new ELEvalException(CommonError.CMN_0104, expression, t.toString(), e);
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

  public List<ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public List<ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }
}
