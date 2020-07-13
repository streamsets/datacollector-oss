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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.util.SysInfo;
import com.streamsets.pipeline.api.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StatsInfo {
  private static final Logger LOG = LoggerFactory.getLogger(StatsInfo.class);

  public static final String SDC_ID = "sdcId";
  public static final String PRODUCT_NAME = "productName";
  public static final String DATA_COLLECTOR_VERSION = "dataCollectorVersion";
  public static final String BUILD_REPO_SHA = "buildRepoSha";
  public static final String DPM_ENABLED = "dpmEnabled";
  public static final String EXTRA_INFO = "extraInfo";

  public static final long PERIOD_OF_TIME_TO_KEEP_STATS_IN_MILLIS = TimeUnit.DAYS.toMillis(90);
  private final ReadWriteLock rwLock;
  private volatile ActiveStats activeStats;
  private List<StatsBean> collectedStats;

  /**
   * @param extensions Immutable list of callbacks to extend stats behavior.
   */
  public StatsInfo(List<AbstractStatsExtension> extensions) {
    rwLock = new ReentrantReadWriteLock(true);
    doWithLock(() -> {
      collectedStats = new ArrayList<>();
      extensions.stream().forEach(e -> e.setStatsInfo(this));
      activeStats = new ActiveStats(extensions.stream().map(AbstractStatsExtension::snapshotAndPopulateStatsInfo).collect(Collectors.toList()));
      activeStats.setStatsInfo(this);
    }, true);
  }

  // for serialization
  private StatsInfo() {
    this(ImmutableList.of());
  }

  public ActiveStats getActiveStats() {
    return activeStats;
  }

  public void setActiveStats(ActiveStats activeStats) {
    this.activeStats = activeStats;
    activeStats.setStatsInfo(this);
  }

  public List<StatsBean> getCollectedStats() {
    return collectedStats;
  }

  protected void doWithLock(Runnable runnable, boolean writeLock) {
    Lock lock = (writeLock) ? rwLock.writeLock() : rwLock.readLock();
    lock.lock();
    try {
      runnable.run();
    } finally {
      lock.unlock();
    }
  }

  public void setCollectedStats(List<StatsBean> collectedStats) {
    this.collectedStats = new ArrayList<>(collectedStats);
  }

  public void startSystem() {
    doWithLock(() -> getActiveStats().startSystem(), false);
  }

  public void stopSystem() {
    doWithLock(() -> getActiveStats().stopSystem(), false);
  }

  public void createPipeline(String pipelineId) {
    doWithLock(() -> getActiveStats().createPipeline(pipelineId), false);
  }

  public void previewPipeline(String pipelineId) {
    doWithLock(() -> getActiveStats().previewPipeline(pipelineId), false);
  }

  public void startPipeline(PipelineConfiguration pipeline) {
    doWithLock(() -> getActiveStats().startPipeline(pipeline), false);
  }

  public void stopPipeline(PipelineConfiguration pipeline) {
    doWithLock(() -> getActiveStats().stopPipeline(pipeline), false);
  }

  public void incrementRecordCount(long count) {
    doWithLock(() -> getActiveStats().incrementRecordCount(count), false);
  }

  public void previewStatusChanged(PreviewStatus previewStatus, Previewer previewer) {
    doWithLock(() -> getActiveStats().previewStatusChanged(previewStatus, previewer), false);
  }

  public void pipelineStatusChanged(PipelineStatus pipelineStatus, PipelineConfiguration conf, Pipeline pipeline) {
    doWithLock(() -> getActiveStats().pipelineStatusChanged(pipelineStatus, conf, pipeline), false);
  }

  /**
   * Track that given error code was used.
   */
  void errorCode(ErrorCode errorCode) {
    doWithLock(() -> getActiveStats().errorCode(errorCode), false);
  }

  /**
   * Provide a consistent hash for a given pipelineId. Helpful to produce anonymous ids that can be used
   * to correlate across periods and across extensions / base DC telemetry.
   *
   * @param pipelineId pipeline ID to hash
   * @return hashed pipeline ID
   */
  String hashPipelineId(String pipelineId) {
    // TODO add a salt, ideally per-pipeline
    return computeHash(pipelineId);
  }

  private static String computeHash(String value) {
    return Hashing.sha256().newHasher().putString(value, Charset.forName("UTF-8")).hash().toString();
  }

  Map<String, Object> getExtraInfo(SysInfo sysInfo) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    sysInfo.toMap().entrySet().forEach(entry -> {
      if (entry.getValue() != null) {
        builder.put(entry.getKey(), entry.getValue());
      }
    });
    return builder.build();
  }

  // if any of the system info changes, it triggers a stats roll.
  Map<String, Object> getCurrentSystemInfo(BuildInfo buildInfo, RuntimeInfo runtimeInfo, SysInfo sysInfo,
                                           Activation activation) {
    return ImmutableMap.<String, Object>builder()
        .put(SDC_ID, runtimeInfo.getId())
        .put(PRODUCT_NAME, runtimeInfo.getProductName())
        .put(DATA_COLLECTOR_VERSION, buildInfo.getVersion())
        .put(BUILD_REPO_SHA, buildInfo.getBuiltRepoSha())
        .put(DPM_ENABLED, runtimeInfo.isDPMEnabled())
        .put(EXTRA_INFO, getExtraInfo(sysInfo))
        .put(ActivationInfo.class.getCanonicalName(), new ActivationInfo(activation))
        .build();
  }

  void setCurrentSystemInfo(BuildInfo buildInfo, RuntimeInfo runtimeInfo, SysInfo sysInfo, Activation activation) {
    Map<String, Object> currentSystemInfo = getCurrentSystemInfo(buildInfo, runtimeInfo, sysInfo, activation);
    setSystemInfo(currentSystemInfo, getActiveStats());
  }

  Map<String, Object> getSystemInfo(ActiveStats stats) {
    ImmutableMap.Builder<String, Object> systemInfoBuilder = new ImmutableMap.Builder<>();
    Optional.ofNullable(stats.getSdcId()).ifPresent(s -> systemInfoBuilder.put(SDC_ID, s));
    Optional.ofNullable(stats.getProductName()).ifPresent((s -> systemInfoBuilder.put(PRODUCT_NAME, s)));
    Optional.ofNullable(stats.getDataCollectorVersion()).ifPresent(s -> systemInfoBuilder.put(DATA_COLLECTOR_VERSION, s));
    Optional.ofNullable(stats.getBuildRepoSha()).ifPresent(s -> systemInfoBuilder.put(BUILD_REPO_SHA, s));
    Optional.ofNullable(stats.getExtraInfo()).ifPresent(s -> systemInfoBuilder.put(EXTRA_INFO, s));
    systemInfoBuilder.put(ActivationInfo.class.getCanonicalName(), stats.getActivationInfo());
    systemInfoBuilder.put(DPM_ENABLED, stats.isDpmEnabled());
    return systemInfoBuilder.build();
  }

  void setSystemInfo(Map<String, Object> info, ActiveStats stats) {
    stats.setSdcId((String) info.get(SDC_ID));
    stats.setProductName((String) info.get(PRODUCT_NAME));
    stats.setDataCollectorVersion((String) info.get(DATA_COLLECTOR_VERSION));
    stats.setBuildRepoSha((String) info.get(BUILD_REPO_SHA));
    stats.setDpmEnabled((Boolean) info.get(DPM_ENABLED));
    stats.setExtraInfo((Map<String, Object>) info.get(EXTRA_INFO));
    stats.setActivationInfo((ActivationInfo) info.get(ActivationInfo.class.getCanonicalName()));
  }

  public boolean rollIfNeeded(
          BuildInfo buildInfo,
          RuntimeInfo runtimeInfo,
          SysInfo sysInfo,
          Activation activation,
          long rollFrequencyMillis,
          boolean forceNextRoll,
          long currentTimeMillis) {
    Map<String, Object> currentSys = getCurrentSystemInfo(buildInfo, runtimeInfo, sysInfo, activation);

    boolean existingStats = getActiveStats().getDataCollectorVersion() != null && !getActiveStats().getDataCollectorVersion().isEmpty();
    Map<String, Object> filteredNewSys = filterSystemInfoDynamicEntries(getSystemInfo(getActiveStats()));
    boolean sysChange = existingStats && !filterSystemInfoDynamicEntries(currentSys).equals(filteredNewSys);

    boolean overFrequency = existingStats &&
        (currentTimeMillis - getActiveStats().getStartTime() > (rollFrequencyMillis * 0.99));
    boolean roll = forceNextRoll || !existingStats || sysChange || overFrequency;
    LOG.debug("Rolling {} due to isForceNextReport()={} || !(existingStats={}) || sysChange={} || overFrequency={}",
        roll, forceNextRoll, existingStats, sysChange, overFrequency);
    if (sysChange) {
      LOG.debug("previous sys, filtered: {}", filterSystemInfoDynamicEntries(currentSys));
      LOG.debug("new sys: {}", filteredNewSys);
    }
    if (roll) {
      doWithLock(() -> {
        if (!existingStats) {
          ActiveStats activeStats = getActiveStats().roll();
          setSystemInfo(currentSys, activeStats);
          setActiveStats(activeStats);
        } else {
          // remove stats older than 90 days
          List<StatsBean> kept = getCollectedStats().stream()
              .filter(s -> currentTimeMillis - s.getEndTime() <= PERIOD_OF_TIME_TO_KEEP_STATS_IN_MILLIS)
              .collect(Collectors.toCollection(ArrayList::new));

          // roll and add active stats
          ActiveStats currentActiveStats = getActiveStats();
          setActiveStats(currentActiveStats.roll());
          // setting the end time of the stats we are storing for collection
          currentActiveStats.setEndTime(getActiveStats().getStartTime());

          kept.add(new StatsBean(runtimeInfo.getId(), currentActiveStats));

          setCollectedStats(kept);

          if (sysChange) {
            setSystemInfo(currentSys, getActiveStats());
          }
        }
      }, true);
    }
    return roll;
  }

  /**
   * Filter out system info for dynamic entries - entries which will normally change throughout the lifecycle of this
   * JVM.
   * @param sysInfo to filter
   * @return map with relatively stable settings
   */
  private Map<String, Object> filterSystemInfoDynamicEntries(Map<String, Object> sysInfo) {
    return sysInfo.entrySet().stream()
        .filter(x -> !x.getKey().equals(EXTRA_INFO) || x.getKey().equals(ActivationInfo.class.getCanonicalName()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public StatsInfo snapshot() {
    StatsInfo[] snapshotHolder = new StatsInfo[1];
    doWithLock(() -> {
      StatsInfo snapshot = new StatsInfo(activeStats.getExtensions());
      snapshot.setActiveStats(activeStats.snapshot());
      snapshot.setCollectedStats(getCollectedStats());
      snapshotHolder[0] = snapshot;
    }, false);
    return snapshotHolder[0];
  }

  public void reset() {
    doWithLock(() -> {
      setActiveStats(getActiveStats().roll());
      getCollectedStats().clear();
    }, true);
  }

}
