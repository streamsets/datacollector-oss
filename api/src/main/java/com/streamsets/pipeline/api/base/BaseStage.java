/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEval;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseStage<C extends Context> implements Stage<C> {
  private Info info;
  private C context;
  private boolean requiresSuperInit;
  private boolean superInitCalled;

  @Override
  public List<ConfigIssue> validateConfigs(Info info, C context)  throws StageException {
    this.info = info;
    this.context = context;
    return validateConfigs();
  }

  public List<ELEval> getElEvals(ElEvalProvider elEvalProvider) {
    return new ArrayList<>();
  }

  protected List<ConfigIssue> validateConfigs()  throws StageException {
    return new ArrayList<>();
  }

  @Override
  public final void init(Info info, C context) throws StageException {
    this.info = info;
    this.context = context;
    init();
    if (requiresSuperInit && !superInitCalled) {
      throw new IllegalStateException("The stage implementation overridden the init() but didn't call super.init()");
    }
  }

  void setRequiresSuperInit() {
    requiresSuperInit = true;
  }

  void setSuperInitCalled() {
    superInitCalled = true;
  }

  protected Info getInfo() {
    return info;
  }

  protected C getContext() {
    return context;
  }

  protected void init() throws StageException {
  }

  @Override
  public void destroy() {
  }

}
