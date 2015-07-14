/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.StageException;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseStage<C extends Context> implements Stage<C> {
  private Info info;
  private C context;
  private boolean requiresSuperInit;
  private boolean superInitCalled;

  @Override
  public List<ConfigIssue> init(Info info, C context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.info = info;
    this.context = context;
    try {
      issues.addAll(init());
      if (issues.isEmpty()) {
        try {
          initX();
        } catch (Exception ex) {
          issues.add(context.createConfigIssue(null, null, Errors.API_19, ex.getMessage()));
        }
      }
      return issues;
    } finally {
      if (requiresSuperInit && !superInitCalled) {
        issues.add(context.createConfigIssue(null, null, Errors.API_20));
      }
    }
  }

  protected List<ConfigIssue> init() {
    return new ArrayList<>();
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

  protected void initX() throws StageException {
  }

  @Override
  public void destroy() {
  }

}
