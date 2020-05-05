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

import java.util.Objects;

public class PipelineRunReport {
  private String hashedId;
  private long startTime;
  private long runMillis;
  private long runnerCount;
  private String executionMode;
  private String finalState; // null until pipeline is in a terminal state

  // for serialization
  public PipelineRunReport() { }

  public PipelineRunReport(String hashedId, PipelineRunStats rs) {
    this.hashedId = hashedId;
    startTime = rs.getStartTime();
    runMillis = rs.getTimer().getAccumulatedTime();
    runnerCount = rs.getRunnerCount() > 0 ? rs.getRunnerCount() : 1;
    executionMode = rs.getExecutionMode();
    finalState = rs.getFinalState();
  }

  /** Hashed pipeline ID */
  public String getHashedId() {
    return hashedId;
  }

  public PipelineRunReport setHashedId(String hashedId) {
    this.hashedId = hashedId;
    return this;
  }

  public long getStartTime() {
    return startTime;
  }

  public PipelineRunReport setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public long getRunMillis() {
    return runMillis;
  }

  public PipelineRunReport setRunMillis(long runMillis) {
    this.runMillis = runMillis;
    return this;
  }

  public long getRunnerCount() {
    return runnerCount;
  }

  public PipelineRunReport setRunnerCount(long runnerCount) {
    this.runnerCount = runnerCount;
    return this;
  }

  public String getExecutionMode() {
    return executionMode;
  }

  public PipelineRunReport setExecutionMode(String executionMode) {
    this.executionMode = executionMode;
    return this;
  }

  public String getFinalState() {
    return finalState;
  }

  public PipelineRunReport setFinalState(String finalState) {
    this.finalState = finalState;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipelineRunReport that = (PipelineRunReport) o;
    return startTime == that.startTime &&
        runMillis == that.runMillis &&
        runnerCount == that.runnerCount &&
        Objects.equals(hashedId, that.hashedId) &&
        Objects.equals(executionMode, that.executionMode) &&
        Objects.equals(finalState, that.finalState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hashedId, startTime, runMillis, runnerCount, executionMode, finalState);
  }
}
