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

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// stats bean that is send back to StreamSets.
public class StatsBean {
  private boolean reported;
  private String sdcId;
  private String productName;
  private String version;
  private String dataCollectorVersion;
  private String buildRepoSha;
  private Map<String, Object> extraInfo;
  private boolean dpmEnabled;
  private long startTime;
  private long endTime;
  private long upTime;
  private long activePipelines;
  private long pipelineMilliseconds;
  private List<PipelineRunReport> pipelineRunReports;
  private Map<String, Long> stageMilliseconds;
  private long recordsOM;
  private Map<String, Long> errorCodes;
  private List<FirstPipelineUse> createToPreview;
  private List<FirstPipelineUse> createToRun;
  private List<StatsBeanExtension> extensions;
  private ActivationInfo activationInfo;

  public StatsBean() {
    stageMilliseconds = new HashMap<>();
  }

  public StatsBean(String sdcId, ActiveStats activeStats) {
    this();
    setSdcId(sdcId);
    setProductName(activeStats.getProductName());
    setVersion(activeStats.getVersion());
    setDataCollectorVersion(activeStats.getDataCollectorVersion());
    setBuildRepoSha(activeStats.getBuildRepoSha());
    setExtraInfo(activeStats.getExtraInfo());
    setDpmEnabled(activeStats.isDpmEnabled());
    setStartTime(activeStats.getStartTime());
    setEndTime(activeStats.getEndTime());
    setUpTime(activeStats.getUpTime().getAccumulatedTime());

    long pipelineMilliseconds = 0;
    setActivePipelines(activeStats.getDeprecatedPipelines().size() + activeStats.getPipelineStats().size());
    for (UsageTimer timer : activeStats.getDeprecatedPipelines()) {
      pipelineMilliseconds += timer.getAccumulatedTime();
    }
    List<PipelineRunReport> runReports = new ArrayList<>();
    for (Map.Entry<String, PipelineStats> entry : activeStats.getPipelineStats().entrySet()) {
      String hashedId = activeStats.hashPipelineId(entry.getKey());
      PipelineStats ps = entry.getValue();
      for (PipelineRunStats run : ps.getRuns()) {
        pipelineMilliseconds += run.getTimer().getAccumulatedTime();
        runReports.add(new PipelineRunReport(hashedId, run));
      }
    }
    setPipelineMilliseconds(pipelineMilliseconds);
    setPipelineRunReports(runReports);

    for (UsageTimer timer : activeStats.getStages()) {
      getStageMilliseconds().put(timer.getName(), timer.getAccumulatedTime());
    }
    if (activeStats.getRecordCount() > 0) {
      setRecordsOM((long) Math.log10(activeStats.getRecordCount()));
    } else {
      setRecordsOM(-1); // no records
    }
    setErrorCodes(new HashMap<>(activeStats.getErrorCodes()));
    createToPreview = getFirstUse(activeStats.getCreateToPreview());
    createToRun = getFirstUse(activeStats.getCreateToRun());
    extensions = activeStats.getExtensions().stream()
        .map(AbstractStatsExtension::report)
        .collect(Collectors.toList());

    activationInfo = activeStats.getActivationInfo();
  }

  @NotNull
  List<FirstPipelineUse> getFirstUse(Map<String, FirstPipelineUse> map) {
    return map.entrySet()
        .stream()
        .filter(e -> e.getValue().getFirstUseOn() > 0)
        .map(e -> e.getValue())
        .collect(Collectors.toList());
  }

  // it will only be serialized to JSON if TRUE, when we report it is still false, thus we don't introduce any
  // change in the telemetry payload.
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isReported() {
    return reported;
  }

  public StatsBean setReported() {
    this.reported = true;
    return this;
  }

  public String getSdcId() {
    return sdcId;
  }

  public StatsBean setSdcId(String sdcId) {
    this.sdcId = sdcId;
    return this;
  }

  public String getProductName() { return productName; }

  public StatsBean setProductName(String productName) {
    this.productName = productName;
    return this;
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

  public String getBuildRepoSha() {
    return buildRepoSha;
  }

  public StatsBean setBuildRepoSha(String buildRepoSha) {
    this.buildRepoSha = buildRepoSha;
    return this;
  }

  public Map<String, Object> getExtraInfo() {
    return extraInfo;
  }

  public StatsBean setExtraInfo(Map<String, Object> extraInfo) {
    this.extraInfo = extraInfo;
    return this;
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

  public List<PipelineRunReport> getPipelineRunReports() { return pipelineRunReports; }

  public void setPipelineRunReports(List<PipelineRunReport> pipelineRunReports) { this.pipelineRunReports = pipelineRunReports; }

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

  public Map<String, Long> getErrorCodes() {
    return errorCodes;
  }

  public void setErrorCodes(Map<String, Long> errorCodes) {
    this.errorCodes = errorCodes;
  }

  public List<FirstPipelineUse> getCreateToPreview() {
    return createToPreview;
  }

  public StatsBean setCreateToPreview(List<FirstPipelineUse> createToPreview) {
    this.createToPreview = createToPreview;
    return this;
  }

  public List<FirstPipelineUse> getCreateToRun() {
    return createToRun;
  }

  public StatsBean setCreateToRun(List<FirstPipelineUse> createToRun) {
    this.createToRun = createToRun;
    return this;
  }

  public List<StatsBeanExtension> getExtensions() {
    return extensions;
  }

  public StatsBean setExtensions(List<StatsBeanExtension> extensions) {
    this.extensions = extensions;
    return this;
  }

  public ActivationInfo getActivationInfo() {
    return activationInfo;
  }

  public StatsBean setActivationInfo(ActivationInfo activationInfo) {
    this.activationInfo = activationInfo;
    return this;
  }
}
