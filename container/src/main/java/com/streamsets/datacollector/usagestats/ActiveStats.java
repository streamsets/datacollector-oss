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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.pipeline.api.ErrorCode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ActiveStats {
  private final Logger LOG = LoggerFactory.getLogger(ActiveStats.class);

  public static final String VERSION = "1.3";

  /** In upgrade scenarios, this can differ from {@link #VERSION} until the first roll. */
  private String version;
  private StatsInfo statsInfo;
  private long startTime;
  private long endTime;
  private String sdcId;
  private String productName;
  private String dataCollectorVersion;
  private String buildRepoSha;
  private Map<String, Object> extraInfo;
  private boolean dpmEnabled;
  private UsageTimer upTime;
  /** This should only be populated in version 1.1 or earlier (and earlier builds of 1.2). It is immediately rolled and
   * never populated again */
  @Deprecated
  private ConcurrentHashMap<String, UsageTimer> deprecatedPipelines;
  private ConcurrentHashMap<String, PipelineStats> pipelineStats;
  private ConcurrentHashMap<String, UsageTimer> stages;
  private AtomicLong recordCount;
  private ConcurrentHashMap<String, Long> errorCodes;
  private ConcurrentHashMap<String, FirstPipelineUse> createToPreview;
  private ConcurrentHashMap<String, FirstPipelineUse> createToRun;
  private List<AbstractStatsExtension> extensions;
  private ActivationInfo activationInfo;

  /**
   * @param extensions immutable list of callbacks for extensions to customize behavior
   */
  @JsonCreator
  public ActiveStats(
      @JsonProperty("extensions") List<AbstractStatsExtension> extensions) {
    version = VERSION;
    startTime = System.currentTimeMillis();
    upTime = new UsageTimer().setName("upTime");
    deprecatedPipelines = new ConcurrentHashMap<>();
    pipelineStats = new ConcurrentHashMap<>();
    stages = new ConcurrentHashMap<>();
    recordCount = new AtomicLong();
    dataCollectorVersion = "";
    errorCodes = new ConcurrentHashMap<>();
    createToPreview = new ConcurrentHashMap<>();
    createToRun = new ConcurrentHashMap<>();
    this.extensions = null == extensions ? Collections.emptyList() : extensions;
    activationInfo = new ActivationInfo(null);
  }

  @JsonIgnore
  public ActiveStats setStatsInfo(StatsInfo statsInfo) {
    this.statsInfo = statsInfo;
    return this;
  }

  public List<AbstractStatsExtension> getExtensions() {
    return extensions;
  }

  public ActiveStats setExtensions(List<AbstractStatsExtension> callbacks) {
    this.extensions = callbacks;
    return this;
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
    return version;
  }

  public ActiveStats setVersion(String version) {
    this.version = version;
    return this;
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

  public Map<String, PipelineStats> getPipelineStats() {
    return pipelineStats;
  }

  public ActiveStats setPipelineStats(Map<String, PipelineStats> pipelineStats) {
    this.pipelineStats.clear();
    this.pipelineStats.putAll(pipelineStats);
    return this;
  }

  /**
   * @deprecated in 1.2, use pipelineStats instead
   */
  @Deprecated
  @JsonProperty(value="pipelines")
  public ActiveStats setDeprecatedPipelines(List<UsageTimer> pipelines) {
    this.deprecatedPipelines.clear();
    for (UsageTimer usageTimer : pipelines) {
      this.deprecatedPipelines.put(usageTimer.getName(), usageTimer);
    }
    return this;
  }

  /**
   * @deprecated in 1.2, use pipelineStats instead
   */
  @Deprecated
  @JsonProperty(value="pipelines")
  public List<UsageTimer> getDeprecatedPipelines() {
    return new ArrayList<>(deprecatedPipelines.values());
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
    safeInvokeCallbacks("startSystem", c -> c.startSystem(this));
    return this;
  }

  public ActiveStats stopSystem() {
    upTime.stop();
    safeInvokeCallbacks("stopSystem", c -> c.stopSystem(this));
    return this;
  }

  public ActiveStats setErrorCodes(Map<String, Long> errorCodes) {
    this.errorCodes.clear();
    this.errorCodes.putAll(errorCodes);
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

  public ActivationInfo getActivationInfo() {
    return activationInfo;
  }

  public ActiveStats setActivationInfo(ActivationInfo activationInfo) {
    this.activationInfo = activationInfo;
    return this;
  }

  public String hashPipelineId(String id) {
    return statsInfo.hashPipelineId(id);
  }

  public ActiveStats createPipeline(String pipelineId) {
    long now = System.currentTimeMillis();
    createToPreview.put(pipelineId, new FirstPipelineUse().setCreatedOn(now));
    createToRun.put(pipelineId, new FirstPipelineUse().setCreatedOn(now));
    safeInvokeCallbacks("createPipeline", c -> c.createPipeline(this, pipelineId));
    return this;
  }

  public ActiveStats previewPipeline(String pipelineId) {
    FirstPipelineUse created = createToPreview.get(pipelineId);
    if (created != null) {
      if (created.getFirstUseOn() == -1) {
        created.setFirstUseOn(System.currentTimeMillis());
      }
    }
    safeInvokeCallbacks("previewPipeline", c -> c.previewPipeline(this, pipelineId));
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
    safeInvokeCallbacks("startPipeline", c -> c.startPipeline(this, pipeline));
    return this;
  }

  public ActiveStats stopPipeline(PipelineConfiguration pipeline) {
    LOG.debug("Stopping UsageTimers for '{}' pipeline and its stages", pipeline.getPipelineId());
    safeInvokeCallbacks("stopPipeline", c -> c.stopPipeline(this, pipeline));
    return this;
  }

  public void errorCode(ErrorCode errorCode) {
    errorCodes.merge(errorCode.getCode(), 1L, (k, v) -> v + 1);
  }

  public ActiveStats incrementRecordCount(long count) {
    recordCount.addAndGet(count);
    return this;
  }

  public ActiveStats previewStatusChanged(PreviewStatus previewStatus, Previewer previewer) {
    LOG.trace("Preview stateChange to {} with pipeline id {}",
        previewStatus, previewer.getName());

    safeInvokeCallbacks("previewStatusChanged", c ->
        c.previewStatusChanged(this, previewStatus, previewer));
    return this;
  }

  public ActiveStats pipelineStatusChanged(PipelineStatus pipelineStatus, PipelineConfiguration conf, Pipeline pipeline) {
    LOG.trace("Pipeline stateChange for {} to {} with runnerCount {}",
        conf == null ? "null" : conf.getPipelineId(),
        pipelineStatus,
        pipeline == null ? "null" : pipeline.getNumOfRunners());

    if (pipelineStatus.isActive()) {
      pipelineActive(pipelineStatus, conf, pipeline);
    } else {
      pipelineInactive(pipelineStatus, conf, pipeline);
    }
    safeInvokeCallbacks("pipelineStatusChanged", c ->
        c.pipelineStatusChanged(this, pipelineStatus, conf, pipeline));
    return this;
  }

  /** Called when pipeline enters any active status */
  private void pipelineActive(PipelineStatus status, PipelineConfiguration config, Pipeline p) {
    if (config == null) {
      // not enough context to do anything
      return;
    }
    String pid = config.getPipelineId();
    PipelineStats statsEntry = pipelineStats.computeIfAbsent(pid, k -> new PipelineStats());
    boolean netNewRun = statsEntry.pipelineActive(status, config, p);

    if (netNewRun) {
      for (StageConfiguration stageConfiguration : config.getStages()) {
        String name = stageConfiguration.getLibrary() + "::" + stageConfiguration.getStageName();
        UsageTimer stageUsageTimer = stages.computeIfAbsent(
            name,
            key -> new UsageTimer().setName(name)
        );
        stageUsageTimer.start();
      }
    }
  }

  /** Called when pipeline enters any non-active status. */
  private void pipelineInactive(PipelineStatus finalState, PipelineConfiguration conf, Pipeline p) {
    if (conf == null) {
      // not enough context to do anything
      return;
    }
    String pid = conf.getPipelineId();
    boolean stopStageTimers = false;
    PipelineStats statsEntry = pipelineStats.get(pid);
    if (statsEntry != null) {
      stopStageTimers = statsEntry.pipelineInactive(finalState.name(), conf, p);
    }
    if (stopStageTimers) {
      for (StageConfiguration stageConfiguration : conf.getStages()) {
        String name = stageConfiguration.getLibrary() + "::" + stageConfiguration.getStageName();
        UsageTimer stageUsageTimer = stages.get(name);
        if (stageUsageTimer != null) {
          if (!stageUsageTimer.stopIfRunning()) {
            LOG.warn(
                "UsageTimer for '{}' stage not not running on stopPipeline for '{}' pipeline",
                name,
                pid
            );
          }
        } else {
          LOG.warn(
              "UsageTimer for '{}' stage not found on stopPipeline for '{}' pipeline",
              name,
              pid
          );
        }
      }
    }
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
    ActiveStats statsBean = new ActiveStats(rollExtensions())
        .setStatsInfo(statsInfo)
        .setSdcId(getSdcId())
        .setProductName(getProductName())
        .setStartTime(now)
        .setDataCollectorVersion(getDataCollectorVersion())
        .setBuildRepoSha(getBuildRepoSha())
        .setExtraInfo(getExtraInfo())
        .setDpmEnabled(isDpmEnabled())
        .setUpTime(getUpTime().roll())
        .setActivationInfo(getActivationInfo());
    statsBean.setDeprecatedPipelines(
        getDeprecatedPipelines().stream()
            // If multiplier is 0, its not running/used anymore
            .filter(u -> u.getMultiplier() > 0)
            .map(UsageTimer::roll)
            .collect(Collectors.toList())
    );
    statsBean.setPipelineStats(getPipelineStats().entrySet().stream()
        .map(e -> ImmutablePair.of(e.getKey(), e.getValue().roll()))
        .filter(p -> p.getRight().getRuns().size() > 0)
        .collect(Collectors.toConcurrentMap(
            ImmutablePair::getLeft,
            ImmutablePair::getRight,
            (k, v) -> { throw new UnsupportedOperationException("Should be no merges of pipeline stats"); }
    )));
    statsBean.setStages(
        getStages().stream()
            // If multiplier is 0, its not running/used anymore
            .filter(u -> u.getMultiplier() > 0)
            .map(UsageTimer::roll)
            .collect(Collectors.toList())
    );
    long expiredTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
    statsBean.setCreateToPreview(removeUsedAndExpired(getCreateToPreview(), expiredTime));
    statsBean.setCreateToRun(removeUsedAndExpired(getCreateToRun(), expiredTime));
    return statsBean;
  }

  // returns a snapshot for persistency
  public ActiveStats snapshot() {
    ActiveStats snapshot = new ActiveStats(snapshotExtensions())
        .setStatsInfo(statsInfo)
        .setSdcId(getSdcId())
        .setProductName(getProductName())
        .setStartTime(getStartTime())
        .setDataCollectorVersion(getDataCollectorVersion())
        .setBuildRepoSha(getBuildRepoSha())
        .setExtraInfo(getExtraInfo())
        .setDpmEnabled(isDpmEnabled())
        .setErrorCodes(errorCodes)
        .setUpTime(getUpTime().snapshot())
        .setRecordCount(getRecordCount())
        .setActivationInfo(getActivationInfo());
    snapshot.setDeprecatedPipelines(getDeprecatedPipelines().stream().map(UsageTimer::snapshot).collect(Collectors.toList()));
    snapshot.setPipelineStats(getPipelineStats().entrySet().stream().collect(Collectors.toConcurrentMap(
        e -> e.getKey(),
        e -> e.getValue().snapshot(),
        (k, v) -> { throw new UnsupportedOperationException("Should be no merges of pipeline stats"); }
    )));
    snapshot.setStages(getStages().stream().map(UsageTimer::snapshot).collect(Collectors.toList()));
    snapshot.setCreateToPreview(getCreateToPreview());
    snapshot.setCreateToRun(getCreateToRun());
    return snapshot;
  }

  private void safeInvokeCallbacks(String methodName, Consumer<AbstractStatsExtension> lambda) {
    extensions.stream().forEachOrdered(c -> {
      try {
        lambda.accept(c);
      } catch (Exception e) {
        LOG.error("Error processing callback {}.{}", c.getName(), methodName, e);
      }
    });
  }

  private List<AbstractStatsExtension> rollExtensions() {
    return extensions.stream().map(e -> e.rollAndPopulateStatsInfo(this)).collect(Collectors.toList());
  }

  private List<AbstractStatsExtension> snapshotExtensions() {
    return extensions.stream().map(AbstractStatsExtension::snapshotAndPopulateStatsInfo).collect(Collectors.toList());
  }
}
