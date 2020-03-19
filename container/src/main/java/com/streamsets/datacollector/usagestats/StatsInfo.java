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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.ErrorCode;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StatsInfo {
  public static final String SDC_ID = "sdcId";
  public static final String DATA_COLLECTOR_VERSION = "dataCollectorVersion";
  public static final String BUILD_REPO_SHA = "buildRepoSha";
  public static final String DPM_ENABLED = "dpmEnabled";
  public static final String EXTRA_INFO = "extraInfo";

  // How many StatsBean intervals are kept, assuming 1hr intervals, 30 days worth of.
  public static final int INTERVALS_TO_KEEP = 24 * 7 * 30;
  private final ReadWriteLock rwLock;
  private volatile ActiveStats activeStats;
  private List<StatsBean> collectedStats;

  public StatsInfo() {
    rwLock = new ReentrantReadWriteLock(true);
    collectedStats = new ArrayList<>();
    activeStats = new ActiveStats();
  }

  public ActiveStats getActiveStats() {
    return activeStats;
  }

  public void setActiveStats(ActiveStats activeStats) {
    this.activeStats = activeStats;
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

  /**
   * Track that given error code was used.
   */
  void errorCode(ErrorCode errorCode) {
    doWithLock(() -> getActiveStats().errorCode(errorCode), false);
  }

  @VisibleForTesting
  static String computeHash(String value) {
    return Hashing.sha256().newHasher().putString(value, Charset.forName("UTF-8")).hash().toString();
  }

  Map<String, String> getExtraInfo() {
    //TODO  capture information such as cloud provider, instance type, cores, memory, etc
    return Collections.emptyMap();
  }

  // if any of the system info changes, it triggers a stats roll.
  Map<String, Object> getCurrentSystemInfo(BuildInfo buildInfo, RuntimeInfo runtimeInfo) {
    return ImmutableMap.of(
        SDC_ID, runtimeInfo.getId(),
        DATA_COLLECTOR_VERSION, buildInfo.getVersion(),
        BUILD_REPO_SHA, buildInfo.getBuiltRepoSha(),
        DPM_ENABLED, runtimeInfo.isDPMEnabled(),
        EXTRA_INFO, getExtraInfo()
    );
  }

  void setCurrentSystemInfo(BuildInfo buildInfo, RuntimeInfo runtimeInfo) {
    Map<String, Object> currentSystemInfo = getCurrentSystemInfo(buildInfo, runtimeInfo);
    setSystemInfo(currentSystemInfo, getActiveStats());
  }

  Map<String, Object> getSystemInfo(ActiveStats stats) {
    ImmutableMap.Builder<String, Object> systemInfoBuilder = new ImmutableMap.Builder<>();
    Optional.ofNullable(stats.getSdcId()).ifPresent(s -> systemInfoBuilder.put(SDC_ID, s));
    Optional.ofNullable(stats.getDataCollectorVersion()).ifPresent(s -> systemInfoBuilder.put(DATA_COLLECTOR_VERSION, s));
    Optional.ofNullable(stats.getBuildRepoSha()).ifPresent(s -> systemInfoBuilder.put(BUILD_REPO_SHA, s));
    Optional.ofNullable(stats.getExtraInfo()).ifPresent(s -> systemInfoBuilder.put(EXTRA_INFO, s));
    systemInfoBuilder.put(DPM_ENABLED, stats.isDpmEnabled());
    return systemInfoBuilder.build();
  }

  void setSystemInfo(Map<String, Object> info, ActiveStats stats) {
    stats.setSdcId((String) info.get(SDC_ID));
    stats.setDataCollectorVersion((String) info.get(DATA_COLLECTOR_VERSION));
    stats.setBuildRepoSha((String) info.get(BUILD_REPO_SHA));
    stats.setDpmEnabled((Boolean) info.get(DPM_ENABLED));
    stats.setExtraInfo((Map<String, String>) info.get(EXTRA_INFO));
  }

  public boolean rollIfNeeded(BuildInfo buildInfo, RuntimeInfo runtimeInfo, long rollFrequencyMillis) {
    Map<String, Object> currentSys = getCurrentSystemInfo(buildInfo, runtimeInfo);

    boolean existingStats = getActiveStats().getDataCollectorVersion() != null && !getActiveStats().getDataCollectorVersion().isEmpty();
    boolean sysChange = existingStats && !currentSys.equals(getSystemInfo(getActiveStats()));

    boolean overFrequency = existingStats &&
        (System.currentTimeMillis() - getActiveStats().getStartTime() > rollFrequencyMillis);
    boolean roll = !existingStats || sysChange || overFrequency;
    if (roll) {
      doWithLock(() -> {
        if (!existingStats) {
          ActiveStats activeStats = getActiveStats().roll();
          setSystemInfo(currentSys, activeStats);
          setActiveStats(activeStats);
        } else {
          ActiveStats currentActiveStats = getActiveStats();
          setActiveStats(currentActiveStats.roll());
          // setting the end time of the stats we are storing for collection
          currentActiveStats.setEndTime(getActiveStats().getStartTime());
          getCollectedStats().add(new StatsBean(runtimeInfo.getId(), currentActiveStats));
          if (getCollectedStats().size() > INTERVALS_TO_KEEP) {
            getCollectedStats().remove(0);
          }
          if (sysChange) {
            setSystemInfo(currentSys, getActiveStats());
          }
        }
      }, true);
    }
    return roll;
  }

  public StatsInfo snapshot() {
    StatsInfo snapshot = new StatsInfo();
    doWithLock(() -> {
      snapshot.setActiveStats(activeStats.snapshot());
      snapshot.setCollectedStats(getCollectedStats());
    }, false);
    return snapshot;
  }

  public void reset() {
    doWithLock(() -> {
      setActiveStats(getActiveStats().roll());
      getCollectedStats().clear();
    }, true);
  }

}
