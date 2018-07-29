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
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StatsInfo {
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

  public void startPipeline(PipelineConfiguration pipeline) {
    doWithLock(() -> getActiveStats().startPipeline(pipeline), false);
  }

  public void stopPipeline(PipelineConfiguration pipeline) {
    doWithLock(() -> getActiveStats().stopPipeline(pipeline), false);
  }

  public void incrementRecordCount(long count) {
    doWithLock(() -> getActiveStats().incrementRecordCount(count), false);
  }

  @VisibleForTesting
  static String computeHash(String value) {
    return Hashing.sha256().newHasher().putString(value, Charset.forName("UTF-8")).hash().toString();
  }

  public boolean rollIfNeeded(BuildInfo buildInfo, RuntimeInfo runtimeInfo, long rollFrequencyMillis) {
    String currentVersion = buildInfo.getVersion();
    boolean currentDpmEnabled = runtimeInfo.isDPMEnabled();
    boolean existingStats = getActiveStats().getDataCollectorVersion() != null && !getActiveStats().getDataCollectorVersion().isEmpty();
    boolean sysChange = existingStats &&
        (!currentVersion.equals(getActiveStats().getDataCollectorVersion())
            || currentDpmEnabled != getActiveStats().isDpmEnabled());
    boolean overFrequency = existingStats &&
        (System.currentTimeMillis() - getActiveStats().getStartTime() > rollFrequencyMillis);
    boolean roll = !existingStats || sysChange || overFrequency;
    if (roll) {
      doWithLock(() -> {
        if (!existingStats) {
          ActiveStats activeStats = getActiveStats().roll();
          activeStats.setDataCollectorVersion(currentVersion);
          activeStats.setDpmEnabled(currentDpmEnabled);
          setActiveStats(activeStats);
        } else {
          ActiveStats currentActiveStats = getActiveStats();
          setActiveStats(currentActiveStats.roll());
          // setting the end time of the stats we are storing for collection
          currentActiveStats.setEndTime(getActiveStats().getStartTime());
          getCollectedStats().add(new StatsBean(currentActiveStats));
          if (getCollectedStats().size() > 10) {
            getCollectedStats().remove(0);
          }
          if (sysChange) {
            getActiveStats().setDataCollectorVersion(currentVersion);
            getActiveStats().setDpmEnabled(currentDpmEnabled);
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
