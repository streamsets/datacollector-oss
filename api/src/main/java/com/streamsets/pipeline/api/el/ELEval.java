/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.el;

import com.streamsets.pipeline.api.impl.Utils;

public abstract class ELEval {

  public interface Variables {
    public void addVariable(String name, Object value);

    public void addContextVariable(String name, Object value);

    public boolean hasVariable(String name);

    public Object getVariable(String name);

    public boolean hasContextVariable(String name);

    public Object getContextVariable(String name);
  }

  public abstract String getConfigName();

  public abstract Variables createVariables();

  public abstract void parseEL(String el) throws ELEvalException;

  protected abstract <T> T evaluate(Variables vars, String el, Class<T> returnType) throws ELEvalException;

  public  <T> T eval(Variables vars, String el, Class<T> returnType) throws ELEvalException {
    Utils.checkNotNull(vars, "vars");
    Utils.checkNotNull(el, "expression");
    Utils.checkNotNull(returnType, "returnType");
    VARIABLES_IN_SCOPE_TL.set(vars);
    try {
      return evaluate(vars, el, returnType);
    } finally {
      VARIABLES_IN_SCOPE_TL.set(null);
    }
  }

  private final static ThreadLocal<ELEval.Variables> VARIABLES_IN_SCOPE_TL = new ThreadLocal<>();

  public static ELEval.Variables getVariablesInScope() {
    return VARIABLES_IN_SCOPE_TL.get();
  }
}