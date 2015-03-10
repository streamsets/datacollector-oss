/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.el;

import com.streamsets.pipeline.api.impl.Utils;

public abstract class ELEval {

  public abstract String getConfigName();

  public abstract ELVars createVariables();

  protected abstract <T> T evaluate(ELVars vars, String el, Class<T> returnType) throws ELEvalException;

  public  <T> T eval(ELVars vars, String el, Class<T> returnType) throws ELEvalException {
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

  private final static ThreadLocal<ELVars> VARIABLES_IN_SCOPE_TL = new ThreadLocal<>();

  public static ELVars getVariablesInScope() {
    return VARIABLES_IN_SCOPE_TL.get();
  }
}