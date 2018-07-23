/*
 * Copyright 2018 StreamSets Inc.
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
import com.streamsets.datacollector.config.StageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ActiveStats {
  private final Logger LOG = LoggerFactory.getLogger(ActiveStats.class);

  public static final String VERSION = "1.0";

  private long startTime;
  private long endTime;
  private String dataCollectorVersion;
  private boolean dpmEnabled;
  private UsageTimer upTime;
  private Map<String, UsageTimer> pipelines;
  private Map<String, UsageTimer> stages;
  private AtomicLong recordCount;

  public ActiveStats() {
    startTime = System.currentTimeMillis();
    upTime = new UsageTimer().setName("upTime");
    pipelines = new ConcurrentHashMap<>();
    stages = new ConcurrentHashMap<>();
    recordCount = new AtomicLong();
    dataCollectorVersion = "";
  }

  public String getVersion() {
    return VERSION;
  }

  public void setVersion(String version) {
    // NoOp, we need this for Jackson not to complain
  }

  public long getStartTime() {
    return startTime;
  }

  public ActiveStats setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public long getEndTime() {
    return endTime;
  }

  public ActiveStats setEndTime(long endTime) {
    this.endTime = endTime;
    return this;
  }

  public String getDataCollectorVersion() {
    return dataCollectorVersion;
  }

  public ActiveStats setDataCollectorVersion(String version) {
    this.dataCollectorVersion = version;
    return this;
  }

  public boolean isDpmEnabled() {
    return dpmEnabled;
  }

  public ActiveStats setDpmEnabled(boolean dpmEnabled) {
    this.dpmEnabled = dpmEnabled;
    return this;
  }

  public UsageTimer getUpTime() {
    return upTime;
  }

  public ActiveStats setUpTime(UsageTimer upTime) {
    this.upTime = upTime;
    return this;
  }

  public ActiveStats setPipelines(List<UsageTimer> pipelines) {
    this.pipelines.clear();
    for (UsageTimer usageTimer : pipelines) {
      this.pipelines.put(usageTimer.getName(), usageTimer);
    }
    return this;
  }

  public List<UsageTimer> getPipelines() {
    return new ArrayList<>(pipelines.values());
  }

  public ActiveStats setStages(List<UsageTimer> stages) {
    this.stages.clear();
    for (UsageTimer usageTimer : stages) {
      this.stages.put(usageTimer.getName(), usageTimer);
    }
    return this;
  }

  public List<UsageTimer> getStages() {
    return new ArrayList<>(stages.values());
  }

  public long getRecordCount() {
    return recordCount.get();
  }

  public ActiveStats setRecordCount(long recordCount) {
    this.recordCount.set(recordCount);
    return this;
  }

  public ActiveStats startSystem() {
    upTime.start();
    return this;
  }

  public ActiveStats stopSystem() {
    upTime.stop();
    return this;
  }

  public ActiveStats startPipeline(PipelineConfiguration pipeline) {
    LOG.debug("Starting UsageTimers for '{}' pipeline and its stages", pipeline.getPipelineId());
    // we only start the pipeline stats if not running already (to avoid stage stats going out of wak)
    if (pipelines.computeIfAbsent(
        pipeline.getPipelineId(),
        id -> new UsageTimer().setName(pipeline.getPipelineId())).startIfNotRunning()
        ) {
      for (StageConfiguration stageConfiguration : pipeline.getStages()) {
        String name = stageConfiguration.getLibrary() + "::" + stageConfiguration.getStageName();
        stages.computeIfAbsent(name, (key) -> new UsageTimer().setName(name)).start();
      }
    }
    return this;
  }

  public ActiveStats stopPipeline(PipelineConfiguration pipeline) {
    LOG.debug("Stopping UsageTimers for '{}' pipeline and its stages", pipeline.getPipelineId());
    // we only stop the pipeline stats if not running already (to avoid stage stats going out of wak)
    UsageTimer pipelineUsageTimer = pipelines.get(pipeline.getPipelineId());
    if (pipelineUsageTimer != null) {
      if (pipelines.get(pipeline.getPipelineId()).stopIfRunning()) {
        for (StageConfiguration stageConfiguration : pipeline.getStages()) {
          String name = stageConfiguration.getLibrary() + "::" + stageConfiguration.getStageName();
          UsageTimer stageUsageTimer = stages.get(name);
          if (stageUsageTimer != null) {
            if (!stageUsageTimer.stopIfRunning()) {
              LOG.warn(
                  "UsageTimer for '{}' stage not not running on stopPipeline for '{}' pipeline",
                  name,
                  pipeline.getPipelineId()
              );
            }
          } else {
            LOG.warn(
                "UsageTimer for '{}' stage not found on stopPipeline for '{}' pipeline",
                name,
                pipeline.getPipelineId()
            );
          }
        }
      }
    } else {
      LOG.warn("UsageTimer for '{}' pipeline not found", pipeline.getPipelineId());
    }
    return this;
  }

  public ActiveStats incrementRecordCount(long count) {
    recordCount.addAndGet(count);
    return this;
  }

  // returns fresh bean with same UsageTimers just reset to zero accumulated time to be used as the new live stats
  public ActiveStats roll() {
    long now = System.currentTimeMillis();
    setEndTime(now);
    ActiveStats statsBean = new ActiveStats().setStartTime(now)
                                             .setDataCollectorVersion(getDataCollectorVersion())
                                             .setDpmEnabled(isDpmEnabled())
                                             .setUpTime(getUpTime().roll());
    statsBean.setPipelines(getPipelines().stream().map(UsageTimer::roll).collect(Collectors.toList()));
    statsBean.setStages(getStages().stream()
                                   .filter(timer -> timer.getMultiplier() > 0)
                                   .map(UsageTimer::roll)
                                   .collect(Collectors.toList()));
    return statsBean;
  }

  // returns a snapshot for persistency
  public ActiveStats snapshot() {
    ActiveStats snapshot = new ActiveStats().setStartTime(getStartTime())
                                            .setDataCollectorVersion(getDataCollectorVersion())
                                            .setDpmEnabled(isDpmEnabled())
                                            .setUpTime(getUpTime().snapshot())
                                            .setRecordCount(getRecordCount());
    snapshot.setPipelines(getPipelines().stream().map(UsageTimer::snapshot).collect(Collectors.toList()));
    snapshot.setStages(getStages().stream().map(UsageTimer::snapshot).collect(Collectors.toList()));
    return snapshot;
  }

}
