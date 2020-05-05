/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.usagestats;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.runner.Pipeline;

public class PipelineRunStats {
  private long startTime = -1;
  private UsageTimer timer;
  private long runnerCount = -1;
  private String executionMode;
  private String finalState; // null until pipeline is in a terminal state

  public long getStartTime() {
    return startTime;
  }

  public PipelineRunStats setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public UsageTimer getTimer() {
    return timer;
  }

  public PipelineRunStats setTimer(UsageTimer timer) {
    this.timer = timer;
    return this;
  }

  public long getRunnerCount() {
    return runnerCount;
  }

  public PipelineRunStats setRunnerCount(long runnerCount) {
    this.runnerCount = runnerCount;
    return this;
  }

  // convenience method
  public PipelineRunStats setRunnerCount(Pipeline p) {
    if (p != null) {
      setRunnerCount(p.getNumOfRunners());
    }
    return this;
  }

  public String getExecutionMode() {
    return executionMode;
  }

  public PipelineRunStats setExecutionMode(String executionMode) {
    this.executionMode = executionMode;
    return this;
  }

  // convenience method
  public PipelineRunStats setExecutionMode(PipelineConfiguration conf) {
    setExecutionMode(conf.getConfiguration("executionMode").getValue().toString());
    return this;
  }

  public String getFinalState() {
    return finalState;
  }

  public PipelineRunStats setFinalState(String finalState) {
    this.finalState = finalState;
    return this;
  }

  /**
   * Returns fresh object that can be used for the next period. Current object's timers are frozen.
   * @return copy for next period
   */
  public PipelineRunStats roll() {
    return new PipelineRunStats()
        .setStartTime(getStartTime())
        .setTimer(getTimer().roll())
        .setRunnerCount(getRunnerCount())
        .setExecutionMode(getExecutionMode())
        .setFinalState(getFinalState());
  }

  /**
   * Returns a frozen copy of this object
   * @return frozen copy
   */
  public PipelineRunStats snapshot() {
    return new PipelineRunStats()
        .setStartTime(getStartTime())
        .setTimer(getTimer().snapshot())
        .setRunnerCount(getRunnerCount())
        .setExecutionMode(getExecutionMode())
        .setFinalState(getFinalState());
  }
}
