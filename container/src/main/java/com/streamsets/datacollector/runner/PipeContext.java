/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

public class PipeContext implements StagePipe.Context {

  private final RuntimeStats runtimeStats;

  public PipeContext() {
    this.runtimeStats = new RuntimeStats();
  }

  @Override
  public RuntimeStats getRuntimeStats() {
    return runtimeStats;
  }
}
