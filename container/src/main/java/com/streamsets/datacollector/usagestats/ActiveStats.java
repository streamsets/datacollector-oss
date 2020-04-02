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
import com.streamsets.pipeline.api.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ActiveStats {
  private final Logger LOG = LoggerFactory.getLogger(ActiveStats.class);

  public static final String VERSION = "1.1";

  private long startTime;
  private long endTime;
  private String sdcId;
  private String productName;
  private String dataCollectorVersion;
  private String buildRepoSha;
  private Map<String, Object> extraInfo;
  private boolean dpmEnabled;
  private UsageTimer upTime;
  private Map<String, UsageTimer> pipelines;
  private Map<String, UsageTimer> stages;
  private AtomicLong recordCount;
  private Map<String, Long> errorCodes;
  private Map<String, FirstPipelineUse> createToPreview;
  private Map<String, FirstPipelineUse> createToRun;


  public ActiveStats() {
    startTime = System.currentTimeMillis();
    upTime = new UsageTimer().setName("upTime");
    pipelines = new ConcurrentHashMap<>();
    stages = new ConcurrentHashMap<>();
    recordCount = new AtomicLong();
    dataCollectorVersion = "";
    errorCodes = new HashMap<>();
    createToPreview = new ConcurrentHashMap<>();
    createToRun = new ConcurrentHashMap<>();
  }

  public String getSdcId() {
    return sdcId;
  }

  public ActiveStats setSdcId(String sdcId) {
    this.sdcId = sdcId;
    return this;
  }

  public String getProductName() {
    return productName;
  }

  public ActiveStats setProductName(String productName) {
    this.productName = productName;
    return this;
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

  public String getBuildRepoSha() {
    return buildRepoSha;
  }

  public ActiveStats setBuildRepoSha(String buildRepoSha) {
    this.buildRepoSha = buildRepoSha;
    return this;
  }

  public Map<String, Object> getExtraInfo() {
    return extraInfo;
  }

  public ActiveStats setExtraInfo(Map<String, Object> extraInfo) {
    this.extraInfo = extraInfo;
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

  public ActiveStats setErrorCodes(Map<String, Long> errorCodes) {
    this.errorCodes = errorCodes;
    return this;
  }

  public Map<String, Long> getErrorCodes() {
    return Collections.unmodifiableMap(errorCodes);
  }


  public Map<String, FirstPipelineUse> getCreateToPreview() {
    return createToPreview;
  }

  public ActiveStats setCreateToPreview(Map<String, FirstPipelineUse> createToPreview) {
    this.createToPreview = new ConcurrentHashMap<>();
    createToPreview.entrySet().stream().forEach(e -> this.createToPreview.put(e.getKey(), (FirstPipelineUse) e.getValue().clone()));
    return this;
  }

  public Map<String, FirstPipelineUse> getCreateToRun() {
    return createToRun;
  }

  public ActiveStats setCreateToRun(Map<String, FirstPipelineUse> createToRun) {
    this.createToRun = new ConcurrentHashMap<>();
    createToRun.entrySet().stream().forEach(e -> this.createToRun.put(e.getKey(), (FirstPipelineUse) e.getValue().clone()));
    return this;
  }

  public ActiveStats createPipeline(String pipelineId) {
    long now = System.currentTimeMillis();
    createToPreview.put(pipelineId, new FirstPipelineUse().setCreatedOn(now));
    createToRun.put(pipelineId, new FirstPipelineUse().setCreatedOn(now));
    return this;
  }

  public ActiveStats previewPipeline(String pipelineId) {
    FirstPipelineUse created = createToPreview.get(pipelineId);
    if (created != null) {
      if (created.getFirstUseOn() == -1) {
        created.setFirstUseOn(System.currentTimeMillis());
      }
    }
    return this;
  }

  public ActiveStats startPipeline(PipelineConfiguration pipeline) {
    LOG.debug("Starting UsageTimers for '{}' pipeline and its stages", pipeline.getPipelineId());
    FirstPipelineUse created = createToRun.get(pipeline.getPipelineId());
    if (created != null) {
      if (created.getFirstUseOn() == -1) {
        created.setFirstUseOn(System.currentTimeMillis());
        created.setStageCount(pipeline.getStages().size());
      }
    }

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

  public void errorCode(ErrorCode errorCode) {
    errorCodes.merge(errorCode.getCode(), 1L, (k, v) -> v + 1);
  }

  public ActiveStats incrementRecordCount(long count) {
    recordCount.addAndGet(count);
    return this;
  }

  Map<String, FirstPipelineUse> removeUsedAndExpired(Map<String, FirstPipelineUse> map, long expiredTime) {
    return map.entrySet()
        .stream()
        .filter(e -> e.getValue().getFirstUseOn() == -1 && e.getValue().getCreatedOn() > expiredTime)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (f1, f2) -> {
          throw new UnsupportedOperationException();
        }, ConcurrentHashMap::new));
  }

  // returns fresh bean with same UsageTimers just reset to zero accumulated time to be used as the new live stats
  public ActiveStats roll() {
    long now = System.currentTimeMillis();
    setEndTime(now);
    ActiveStats statsBean = new ActiveStats().setSdcId(getSdcId())
                                             .setProductName(getProductName())
                                             .setStartTime(now)
                                             .setDataCollectorVersion(getDataCollectorVersion())
                                             .setBuildRepoSha(getBuildRepoSha())
                                             .setExtraInfo(getExtraInfo())
                                             .setDpmEnabled(isDpmEnabled())
                                             .setErrorCodes(errorCodes)
                                             .setUpTime(getUpTime().roll());
    statsBean.setPipelines(getPipelines().stream().map(UsageTimer::roll).collect(Collectors.toList()));
    statsBean.setStages(getStages().stream()
                                   .filter(timer -> timer.getMultiplier() > 0)
                                   .map(UsageTimer::roll)
                                   .collect(Collectors.toList()));
    long expiredTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
    statsBean.setCreateToPreview(removeUsedAndExpired(getCreateToPreview(), expiredTime));
    statsBean.setCreateToRun(removeUsedAndExpired(getCreateToRun(), expiredTime));
    return statsBean;
  }

  // returns a snapshot for persistency
  public ActiveStats snapshot() {
    ActiveStats snapshot = new ActiveStats().setSdcId(getSdcId())
                                            .setProductName(getProductName())
                                            .setStartTime(getStartTime())
                                            .setDataCollectorVersion(getDataCollectorVersion())
                                            .setBuildRepoSha(getBuildRepoSha())
                                            .setExtraInfo(getExtraInfo())
                                            .setDpmEnabled(isDpmEnabled())
                                            .setErrorCodes(errorCodes)
                                            .setUpTime(getUpTime().snapshot())
                                            .setRecordCount(getRecordCount());
    snapshot.setPipelines(getPipelines().stream().map(UsageTimer::snapshot).collect(Collectors.toList()));
    snapshot.setStages(getStages().stream().map(UsageTimer::snapshot).collect(Collectors.toList()));
    snapshot.setCreateToPreview(getCreateToPreview());
    snapshot.setCreateToRun(getCreateToRun());
    return snapshot;
  }

}
