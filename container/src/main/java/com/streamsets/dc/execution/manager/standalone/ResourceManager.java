/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.manager.standalone;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.util.Configuration;

import javax.inject.Inject;

public class ResourceManager implements StateEventListener {

  private int currentCapacity;

  @Inject
  public ResourceManager(Configuration configuration) {
    currentCapacity = configuration.get(ExecutorConstants.RUNNER_THREAD_POOL_SIZE_KEY,
      ExecutorConstants.RUNNER_THREAD_POOL_SIZE_DEFAULT) * ExecutorConstants.RUNNER_THREAD_POOL_SIZE_MULTIPLIER;
  }

  public boolean requestRunnerResources(ThreadUsage threadUsage) {
    synchronized(this) {
      int newCapacity = threadUsage.reserve(currentCapacity);
      if(newCapacity < 0) {
        return false;
      }
      currentCapacity = newCapacity;
      return true;
    }
  }

  @Override
  public void
    onStateChange(PipelineState fromState, PipelineState toState, String toStateJson, ThreadUsage threadUsage) {
    if ((fromState.getStatus().isActive() && !toState.getStatus().isActive())
      || toState.getStatus() == PipelineStatus.RETRY) {
      synchronized (this) {
        currentCapacity = threadUsage.release(currentCapacity);
      }
    }
  }
}
