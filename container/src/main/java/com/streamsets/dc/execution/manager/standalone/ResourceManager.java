/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.dc.execution.manager.standalone;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.util.Configuration;

import javax.inject.Inject;
import java.util.Map;

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
    onStateChange(
      PipelineState fromState,
      PipelineState toState,
      String toStateJson,
      ThreadUsage threadUsage,
      Map<String, String> offset
  ) {
    if ((fromState.getStatus().isActive() && !toState.getStatus().isActive())
      || toState.getStatus() == PipelineStatus.RETRY) {
      synchronized (this) {
        currentCapacity = threadUsage.release(currentCapacity);
      }
    }
  }
}
