/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.api.impl.Utils;

public class PipelineState {
  private final String name;
  private final String rev;
  private final State state;
  private final String message;
  private final long lastStatusChange;

  public PipelineState(String name, String rev, State state, String message, long lastStatusChange) {
    this.name = name;
    this.rev = rev;
    this.state = state;
    this.message = message;
    this.lastStatusChange = lastStatusChange;
  }

  public String getRev() {
    return rev;
  }

  public State getState() {
    return this.state;
  }

  public String getMessage() {
    return this.message;
  }

  public long getLastStatusChange() {
    return lastStatusChange;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineState[name='{}' rev='{}' state='{}' message='{}' lastStatusChange='{}']",
      getName(), getRev(), getState().name(), getMessage(), getLastStatusChange());
  }
}
