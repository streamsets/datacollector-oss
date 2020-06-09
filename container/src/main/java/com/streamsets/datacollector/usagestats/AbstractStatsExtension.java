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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.runner.Pipeline;

/**
 * Callback for extensions to inject statistics. All methods must be safe for multithreading. Locking is done in
 * StatsInfo that will treat structural changes to StatsInfo fields as requiring a write lock, and any other access,
 * including mutation of ActiveStats, as a read action. All abstract methods are called with some sort of lock, and
 * any additional entry points implemented by a subclass must call {@link #doWithStatsInfoStructuralLock(Runnable)},
 * before accessing or modifying any state.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public abstract class AbstractStatsExtension {

  private StatsInfo statsInfo = null;

  @JsonIgnore
  private StatsInfo getStatsInfo() {
    return statsInfo;
  }

  @VisibleForTesting
  @JsonIgnore
  void setStatsInfo(StatsInfo statsInfo) {
    this.statsInfo = statsInfo;
  }

  final AbstractStatsExtension rollAndPopulateStatsInfo(ActiveStats as) {
    AbstractStatsExtension ret = roll(as);
    ret.setStatsInfo(getStatsInfo());
    return ret;
  }

  final AbstractStatsExtension snapshotAndPopulateStatsInfo() {
    AbstractStatsExtension ret = snapshot();
    ret.setStatsInfo(getStatsInfo());
    return ret;
  }

  /**
   * If subclasses implement any entry points other than the abstract methods in this class, such as asynchronous
   * updates to a local stats cache, then they must call this method to protect concurrent access. This lock mostly
   * protects copy operations (roll and snapshot) from concurrent modification so they get consistent snapshots. All
   * other mutations must be written in a thread-safe manner.
   *
   * @param runnable code to run that will be protected by a lock
   */
  public final void doWithStatsInfoStructuralLock(Runnable runnable) {
    getStatsInfo().doWithLock(runnable, false);
  }

  /**
   * Provide a consistent hash for a given pipelineId. Helpful to produce anonymous ids that can be used
   * to correlate across periods and across extensions / base DC telemetry.
   *
   * @param pipelineId pipeline ID to hash
   * @return hashed pipeline ID
   */
  public final String hashPipelineId(String pipelineId) {
    return getStatsInfo().hashPipelineId(pipelineId);
  }

  /**
   * Identifier for this extension / callback, used for messages but not persistence.
   *
   * @return identifier
   */
  @JsonIgnore
  public String getName() {
    return getClass().getSimpleName();
  }

  /**
   * Return a version, like 1.0 or 1.2.1, that represents the version of the JSON for this extension. Whenever a
   * backwards incompatible change is done (behavioral or structural), this version must be incremented.
   *
   * @return version for this extension's JSON
   */
  protected abstract String getVersion();

  /**
   * Called by deserialization code. If this differs from the current version, must perform upgrades as needed, though
   * there is no guarantee of the order of set calls, so you should use a @JsonConstructor to handle upgrades.
   *
   * @param version version from serialized object
   */
  protected abstract void setVersion(String version);

  /**
   * Called when stats are rolled, which means that a period has ended, the current stats objects should be frozen,
   * and we need to return a fresh stats object for the next period. StatsInfo will take a write lock before calling
   * this.
   *
   * @param activeStats current active stats, before roll has completed, but after endTime is set.
   * @return new copy of this class that will be used to start the next period.
   */
  protected abstract AbstractStatsExtension roll(ActiveStats activeStats);

  /**
   * Collect information worth reporting. Performed after a roll, so state should be frozen, during the same StatsInfo
   * write lock as roll.
   *
   * @return StatsBeanExtension object to be included in reports
   */
  protected abstract StatsBeanExtension report();

  /**
   * Called when stats are snapshotted, usually before they are persisted to disk. Also called during initialization
   * to ensure objects are isolated instances. StatsInfo will take a write lock before calling this.
   *
   * @return new, frozen copy of this class.
   */
  protected abstract AbstractStatsExtension snapshot();

  /**
   * Called when the stats system is initialized, generally around the time the product starts. StatsInfo will take a
   * read lock before calling this.
   *
   * @param activeStats current active stats
   */
  protected void startSystem(ActiveStats activeStats) {
  }

  ;

  /**
   * Called when the stats system is stopped, generally around the time the product shuts down (gracefully). May not
   * be called for abnormal exits. StatsInfo will take a read lock before calling this.
   *
   * @param activeStats current active stats
   */
  protected void stopSystem(ActiveStats activeStats) {
  }

  ;

  /**
   * Called when a pipeline is created. StatsInfo will take a read lock before calling this.
   *
   * @param activeStats current active stats
   */
  protected void createPipeline(ActiveStats activeStats, String pipelineId) {
  }

  ;

  /**
   * Called when a pipeline is previewed. StatsInfo will take a read lock before calling this.
   *
   * @param activeStats current active stats
   */
  protected void previewPipeline(ActiveStats activeStats, String pipelineId) {
  }

  ;

  /**
   * Called when a pipeline is started. StatsInfo will take a read lock before calling this.
   *
   * @param activeStats current active stats
   */
  protected void startPipeline(ActiveStats activeStats, PipelineConfiguration pipeline) {
  }

  ;

  /**
   * Called when a pipeline is stopped. StatsInfo will take a read lock before calling this.
   *
   * @param activeStats current active stats
   */
  protected void stopPipeline(ActiveStats activeStats, PipelineConfiguration pipeline) {
  }

  /**
   * Called when there is a previewer state change. StatsInfo will take a read lock before calling this.
   *
   * @param activeStats   current active stats
   * @param previewStatus New pipeline status
   * @param previewer     current previewer
   */
  protected void previewStatusChanged(
      ActiveStats activeStats,
      PreviewStatus previewStatus,
      Previewer previewer) {
    // invoke legacy implementation for backwards compatibility
    previewStatusChanged(previewStatus, previewer);
  }

  /**
   * @deprecated use {@link #previewStatusChanged(ActiveStats, PreviewStatus, Previewer)}
   */
  @Deprecated
  protected void previewStatusChanged(
      PreviewStatus previewStatus,
      Previewer previewer) {
  }

  /**
   * Called when there is a pipeline run state change. StatsInfo will take a read lock before calling this. conf and
   * pipeline may be stale for the STARTING status, and implementors may want to just ignore the event in that case. A
   * RUNNING event or some other state with full context should follow.
   *
   * @param activeStats    current active stats
   * @param pipelineStatus New pipeline status
   * @param conf           PipelineConfiguration, may be null or may be stale from previous run.
   * @param pipeline       Pipeline object, may be null, or may be stale from the previous run.
   */
  protected void pipelineStatusChanged(
      ActiveStats activeStats,
      PipelineStatus pipelineStatus,
      PipelineConfiguration conf,
      Pipeline pipeline) {
    // invoke legacy implementation for backwards compatibility
    pipelineStatusChanged(pipelineStatus, conf, pipeline);
  }

  /**
   * @deprecated use {@link #pipelineStatusChanged(ActiveStats, PipelineStatus, PipelineConfiguration, Pipeline)}
   */
  @Deprecated
  protected void pipelineStatusChanged(
      PipelineStatus pipelineStatus,
      PipelineConfiguration conf,
      Pipeline pipeline) {
  }
}
