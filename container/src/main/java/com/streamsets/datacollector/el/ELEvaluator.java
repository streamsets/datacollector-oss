/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.el;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.CommonError;
import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ELEvaluator extends ELEval {
  private final static Logger LOG = LoggerFactory.getLogger(ELEvaluator.class);

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
      Set<ElFunctionDefinition> elFunctions = new HashSet<>();
      Set<ElConstantDefinition> elConstants = new HashSet<>();
      for (Class<?> klass : elFuncConstDefClasses) {
        for (Method m : klass.getMethods()) {
          ElFunction elFunctionAnnot = m.getAnnotation(ElFunction.class);
          if (elFunctionAnnot != null) {
            if (!Modifier.isStatic(m.getModifiers())) {
              throw new RuntimeException(Utils.format("EL function method must be static, class:'{}' method:'{}",
                klass.getName(), m));
            }
            //getMethods returns only public methods, so the following is always true
            if (!Modifier.isPublic(m.getModifiers())) {
              throw new RuntimeException(Utils.format("EL function method must be public, class:'{}' method:'{}",
                klass.getName(), m));
            }
            String functionName = elFunctionAnnot.name();
            if (functionName.isEmpty()) {
              throw new RuntimeException(Utils.format("EL function name cannot be empty, class:'{}' method:'{}",
                klass.getName(), m));
            }
            if (!elFunctionAnnot.prefix().isEmpty()) {
              functionName = elFunctionAnnot.prefix() + ":" + functionName;
            }
            if (functions.containsKey(functionName)) {
              LOG.warn("The function '{}' is already defined for '{}', overriding to '{}'", functionName,
                       functions.get(functionName).getName(), m.getName());
            }
            functions.put(functionName, m);
            Annotation[][] parameterAnnotations = m.getParameterAnnotations();
            Class<?>[] parameterTypes = m.getParameterTypes();
            List<ElFunctionArgumentDefinition> elFunctionArgumentDefinitions = new ArrayList<>(
              parameterTypes.length);
            for (int i = 0; i < parameterTypes.length; i++) {
              Annotation annotation = parameterAnnotations[i][0];
              elFunctionArgumentDefinitions.add(new ElFunctionArgumentDefinition(((ElParam) annotation).value(),
                parameterTypes[i].getSimpleName()));
            }
            elFunctions.add(new ElFunctionDefinition(null, elFunctionAnnot.prefix(), functionName,
              elFunctionAnnot.description(),
              elFunctionArgumentDefinitions,
              m.getReturnType().getSimpleName()));
          }
        }
        for (Field f : klass.getFields()) {
          ElConstant elConstant = f.getAnnotation(ElConstant.class);
          if (elConstant != null) {
            if (!Modifier.isStatic(f.getModifiers())) {
              throw new RuntimeException(Utils.format("EL constant field must be static, class:'{}' field:'{}",
                klass.getName(), f));
            }
            //getFields returns only accessible public fields, so the following is always true
            if (!Modifier.isPublic(f.getModifiers())) {
              throw new RuntimeException(Utils.format("EL constant field must be public, class:'{}' field:'{}",
                klass.getName(), f));
            }
            String constantName = elConstant.name();
            if (constantName.isEmpty()) {
              throw new RuntimeException(Utils.format("EL constant name cannot be empty, class:'{}' field:'{}",
                klass.getName(), f));
            }
            try {
              if (constants.containsKey(constantName)) {
                LOG.warn("The constant '{}' is already defined for '{}', overriding to '{}'", constantName,
                         constants.get(constantName), f.get(null));
              }
              constants.put(constantName, f.get(null));
              elConstants.add(new ElConstantDefinition(null, constantName, elConstant.description(),
                f.getType().getSimpleName()));
            } catch (IllegalAccessException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
      //we are using sets to remove duplicate functions and constants
      elFunctionDefinitions.addAll(elFunctions);
      elConstantDefinitions.addAll(elConstants);
    }
  }

  @Override
  public String getConfigName() {
    return configName;
  }

  public ELVars createVariables() {
    ELVars variables = new ELVariables(constants);
    return variables;
  }

  public static void parseEL(String el) throws ELEvalException {
    try {
      EVALUATOR.parseExpressionString(el);
    } catch (ELException e) {
      throw new ELEvalException(CommonError.CMN_0105, el, e.getMessage(), e);
    }
  }

  @Override
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
      throw new ELEvalException(CommonError.CMN_0104, expression, t.getMessage(), e);
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
