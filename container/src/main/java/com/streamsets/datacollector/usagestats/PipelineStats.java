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

import com.google.common.base.Strings;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.runner.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PipelineStats {
  private List<PipelineRunStats> runs = new ArrayList<>();

  public synchronized List<PipelineRunStats> getRuns() {
    return runs;
  }

  public synchronized PipelineStats setRuns(List<PipelineRunStats> runs) {
    this.runs = runs;
    return this;
  }

  /**
   * Mark the given pipeline active. If a previous run was not stopped, no-op.
   * @param status PipelineStatus. If STARTING, then context objects are likely to be null or stale, and should be
   *               ignored other than immutable properties like pipeline id.
   * @param conf PipelineConfiguration
   * @param p Pipeline, may be null
   * @return true if a timer was started
   */
  public boolean pipelineActive(PipelineStatus status, PipelineConfiguration conf, Pipeline p) {
    String pid = conf.getPipelineId();
    long startTime = System.currentTimeMillis();
    synchronized (this) {
      boolean alreadyRunning = false;
      boolean timerStarted = false;
      PipelineRunStats lastRun = getLastRun();
      if (null != lastRun) {
        if (lastRun.getTimer().isRunning()) {
          alreadyRunning = true;
          timerStarted = false;
        } else if (lastRun.getFinalState() == null) {
          // need to start the timer up again, probably after a reboot. Deserialized timers are always stopped.
          lastRun.getTimer().start();
          alreadyRunning = true;
          timerStarted = true;
        }
        if (alreadyRunning) {
          if (!PipelineStatus.STARTING.equals(status)) {
            // only do config updates if non-starting, since STARTING tends to have stale info in the context
            if (lastRun.getRunnerCount() < 0) {
              lastRun.setRunnerCount(p);
            }
            if (lastRun.getExecutionMode() == null) {
              lastRun.setExecutionMode(conf);
            }
          }
          return timerStarted;
        }
      }
      // insert new run record
      PipelineRunStats record = new PipelineRunStats()
          .setStartTime(startTime)
          .setTimer(new UsageTimer().setName(pid).start());
      // only trust exec mode and runner count if not in STARTING state, else they might be stale
      if (!PipelineStatus.STARTING.equals(status)) {
        record.setExecutionMode(conf);
        record.setRunnerCount(p);
      }
      runs.add(record);
      return true;
    }
  }

  /**
   * Mark the given pipeline as inactive.
   * @param finalState final state for the run
   * @param conf PipelineConfiguration
   * @param p Pipeline, may be null
   * @return true if a run was marked as stopped, false if no run was updated (already stopped).
   */
  public synchronized boolean pipelineInactive(String finalState, PipelineConfiguration conf, Pipeline p) {
    return stopLastEntryIfRunning(finalState, conf, p);
  }

  private synchronized boolean stopLastEntryIfRunning(String finalState, PipelineConfiguration conf, Pipeline p) {
    PipelineRunStats lastRun = getLastRun();
    if (null == lastRun || !lastRun.getTimer().isRunning()) {
      return false;
    }
    lastRun.getTimer().stop();
    lastRun.setFinalState(finalState);
    if (lastRun.getRunnerCount() < 0) {
      lastRun.setRunnerCount(p);
    }
    if (Strings.isNullOrEmpty(lastRun.getExecutionMode())) {
      lastRun.setExecutionMode(conf);
    }
    return true;
  }

  private synchronized PipelineRunStats getLastRun() {
    List<PipelineRunStats> runs = getRuns();
    if (runs.isEmpty()) {
      return null;
    }
    return runs.get(runs.size() - 1);
  }

  /**
   * Returns fresh object that can be used for the next period. Current object's timers are frozen.
   * @return copy for next period
   */
  public PipelineStats roll() {
    return new PipelineStats()
        .setRuns(getRuns().stream()
            .filter(r -> r.getFinalState() == null)
            .map(e -> e.roll())
            .collect(Collectors.toList()));
  }

  /**
   * Returns a frozen copy of this object
   * @return frozen copy
   */
  public PipelineStats snapshot() {
    return new PipelineStats()
        .setRuns(getRuns().stream().map(e -> e.snapshot()).collect(Collectors.toList()));
  }
}
