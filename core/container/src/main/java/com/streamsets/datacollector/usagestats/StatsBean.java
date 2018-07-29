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

import java.util.HashMap;
import java.util.Map;

// stats bean that is send back to StreamSets.
public class StatsBean {
  private String version;
  private String dataCollectorVersion;
  private boolean dpmEnabled;
  private long startTime;
  private long endTime;
  private long upTime;
  private long activePipelines;
  private long pipelineMilliseconds;
  private Map<String, Long> stageMilliseconds;
  private long recordsOM;

  public StatsBean() {
    stageMilliseconds = new HashMap<>();
  }

  public StatsBean(ActiveStats activeStats) {
    this();
    setVersion(activeStats.getVersion());
    setDataCollectorVersion(activeStats.getDataCollectorVersion());
    setDpmEnabled(activeStats.isDpmEnabled());
    setStartTime(activeStats.getStartTime());
    setEndTime(activeStats.getEndTime());
    setUpTime(activeStats.getUpTime().getAccumulatedTime());
    long pipelineMilliseconds = 0;
    setActivePipelines(activeStats.getPipelines().size());
    for (UsageTimer timer : activeStats.getPipelines()) {
      pipelineMilliseconds += timer.getAccumulatedTime();
    }
    setPipelineMilliseconds(pipelineMilliseconds);
    for (UsageTimer timer : activeStats.getStages()) {
      getStageMilliseconds().put(timer.getName(), timer.getAccumulatedTime());
    }
    if (activeStats.getRecordCount() > 0) {
      setRecordsOM((long) Math.log10(activeStats.getRecordCount()));
    } else {
      setRecordsOM(-1); // no records
    }
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getDataCollectorVersion() {
    return dataCollectorVersion;
  }

  public void setDataCollectorVersion(String version) {
    this.dataCollectorVersion = version;
  }

  public boolean isDpmEnabled() {
    return dpmEnabled;
  }

  public void setDpmEnabled(boolean dpmEnabled) {
    this.dpmEnabled = dpmEnabled;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getUpTime() {
    return upTime;
  }

  public void setUpTime(long upTime) {
    this.upTime = upTime;
  }

  public long getActivePipelines() {
    return activePipelines;
  }

  public void setActivePipelines(long activePipelines) {
    this.activePipelines = activePipelines;
  }

  public long getPipelineMilliseconds() {
    return pipelineMilliseconds;
  }

  public void setPipelineMilliseconds(long pipelineMilliseconds) {
    this.pipelineMilliseconds = pipelineMilliseconds;
  }

  public Map<String, Long> getStageMilliseconds() {
    return stageMilliseconds;
  }

  public void setStageMilliseconds(Map<String, Long> stageMilliseconds) {
    this.stageMilliseconds = stageMilliseconds;
  }

  public long getRecordsOM() {
    return recordsOM;
  }

  public void setRecordsOM(long recordsOM) {
    this.recordsOM = recordsOM;
  }

}
