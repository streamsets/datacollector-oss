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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.bundles.BundleType;
import com.streamsets.datacollector.bundles.SupportBundle;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StatsCollectorTask extends AbstractTask implements StatsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(StatsCollectorTask.class);

  static final String ROLL_FREQUENCY_CONFIG = "stats.rollFrequency.days";

  private static final int ROLL_FREQUENCY_DEFAULT = 7;

  static final String OPT_FILE = "opt-stats.json";

  static final String STATS_FILE = "stats.json";

  static final String STATS_ACTIVE_KEY = "stats.active";
  static final String STATS_LAST_REPORT_KEY = "stats.lastReport";

  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final long rollFrequencyMillis;
  private final SafeScheduledExecutorService executorService;
  private final SupportBundleManager bundleManager;
  private final File optFile;
  private final File statsFile;
  private boolean opted;
  private volatile boolean active;
  private long lastReport;
  private ScheduledFuture future;
  private volatile StatsInfo statsInfo;

  public StatsCollectorTask(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration config,
      SafeScheduledExecutorService executorService,
      SupportBundleManager bundleManager
  ) {
    super("StatsCollector");
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    rollFrequencyMillis = TimeUnit.DAYS.toMillis(config.get(ROLL_FREQUENCY_CONFIG, ROLL_FREQUENCY_DEFAULT));
    this.executorService = executorService;
    this.bundleManager = bundleManager;
    optFile = new File(runtimeInfo.getDataDir(), OPT_FILE);
    statsFile = new File(runtimeInfo.getDataDir(), STATS_FILE);
  }

  @VisibleForTesting
  protected BuildInfo getBuildInfo() {
    return buildInfo;
  }

  @VisibleForTesting
  protected RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  @VisibleForTesting
  protected SupportBundleManager getBundleManager() {
    return bundleManager;
  }

  @VisibleForTesting
  protected File getOptFile() {
    return optFile;
  }

  @VisibleForTesting
  protected File getStatsFile() {
    return statsFile;
  }

  @VisibleForTesting
  protected long getRollFrequencyMillis() {
    return rollFrequencyMillis;
  }

  @Override
  public StatsInfo getStatsInfo() {
    return statsInfo;
  }

  @Override
  protected void initTask() {
    super.initTask();

    statsInfo =  new StatsInfo();

    if (runtimeInfo.isClusterSlave()) {
      opted = true;
      active = false;
      LOG.debug("Cluster slave, stats collection is disabled");
    } else {
      opted = optFile.exists();
      if (opted) {
        try (InputStream is = new FileInputStream(optFile)) {
          Map map = ObjectMapperFactory.get().readValue(is, Map.class);
          if (map == null) {
            opted = false;
            active = false;
            LOG.warn("Stats collection opt-in not properly set, switching off and re-opting");
          } else {
            if (map.containsKey(STATS_ACTIVE_KEY)) {
              opted = true;
              active = (Boolean) map.get(STATS_ACTIVE_KEY);
            }
            if (active) {
              if (map.containsKey(STATS_LAST_REPORT_KEY)) {
                lastReport = (Long) map.get(STATS_LAST_REPORT_KEY);
              }
            }
          }
        } catch (IOException ex) {
          opted = false;
          active = false;
          LOG.warn("Stats collection opt-in error, switching off and re-opting. Error: {}", ex);
        }
      }
      if (active) {
        if (statsFile.exists()) {
          DataStore ds = new DataStore(statsFile);
          try {
            try (InputStream is = ds.getInputStream()) {
              StatsInfo data = ObjectMapperFactory.get().readValue(is, StatsInfo.class);
              if (data == null) {
                opted = false;
                active = false;
                LOG.warn("Stats collection data is missing, switching off and re-opting");
              } else {
                statsInfo = data;
                LOG.debug("Stats collection loaded");
              }
            }
          } catch (IOException ex) {
            opted = false;
            active = false;
            LOG.warn("Stats collection data is invalid, switching off and re-opting. Error: {}", ex);
          }
        }
      }
      if (!opted) {
        try {
          if (optFile.exists()) {
            if (optFile.delete()) {
              LOG.error("Could not delete opt-in status file. Stats Collection is disabled");
            }
          }
        } catch (Exception ex) {
          LOG.error("Could not delete opt-in status file. Stats Collection is disabled. Error: {}", ex);
        }
      }
      if (!active) {
        try {
          if (statsFile.exists()) {
            if (statsFile.delete()) {
              LOG.error("Could not delete stats collected data file. Stats Collection is disabled.");
            }
          }
        } catch (Exception ex) {
          LOG.error("Could not delete stats collected data file. Stats Collection is disabled. Error: {}", ex);
        }
      }
    }
    if (!getRuntimeInfo().isClusterSlave()) {
      LOG.info("Stats Collection, opted '{}, active '{}'", opted, active);
    }
    // when disabled all persistency/reporting done by the Runnable is a No Op.
    getStatsInfo().startSystem();
    getRunnable().run();
    future = executorService.scheduleAtFixedRate(getRunnable(), 60, 60, TimeUnit.SECONDS);
  }

  Runnable getRunnable() {
    return () -> {
      if (active) {
        if (getStatsInfo().rollIfNeeded(getBuildInfo(), getRuntimeInfo(), getRollFrequencyMillis())) {
          LOG.debug("Stats collection data rolled");
        }
        if (!getStatsInfo().getCollectedStats().isEmpty()) {
          LOG.debug("Reporting");
          if (reportStats(getStatsInfo().getCollectedStats())) {
            LOG.debug("Reported");
            getStatsInfo().getCollectedStats().clear();
          }
        }
        saveStats();
      }
    };
  }

  protected boolean reportStats(List<StatsBean> stats) {
    try {
      getBundleManager().uploadNewBundleFromInstances(
        Collections.singletonList(new StatsGenerator(stats)),
        BundleType.STATS
      );
      return true;
    } catch (IOException ex) {
      LOG.warn("Reporting failed. Error: {}", ex);
      return false;
    }
  }

  protected void saveStats() {
    DataStore ds = new DataStore(statsFile);
    try {
      try (OutputStream os = ds.getOutputStream()) {
        ObjectMapperFactory.get().writeValue(os, getStatsInfo().snapshot());
        ds.commit(os);
        LOG.debug("Saved stats collections");
      }
    } catch (IOException ex) {
      opted = false;
      active = false;
      LOG.warn("Could not save stats collection, Disabling and re-opting. Error: {}", ex);
    } finally {
      ds.release();
    }
  }

  @Override
  protected void stopTask() {
    if (getFuture() != null) {
      getFuture().cancel(false);
    }
    getStatsInfo().stopSystem();
    getRunnable().run();
    super.stopTask();
  }

  @Override
  public boolean isOpted() {
    return opted;
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void setActive(boolean active) {
    if (!isOpted() || isActive() != active) {
      LOG.info("Setting stats collection to '{}'", active);
      try (OutputStream os = new FileOutputStream(optFile)) {
        ObjectMapperFactory.get().writeValue(
            os,
            ImmutableMap.of(STATS_ACTIVE_KEY, active, STATS_LAST_REPORT_KEY, System.currentTimeMillis())
        );
        this.active = active;
        opted = true;
      } catch (IOException ex) {
        this.active = false;
        opted = false;
        LOG.warn("Could not change stats collection state, Disabling and re-opting. Error: {}", ex);
      }
      getStatsInfo().reset();
      saveStats();
    }
  }

  @Override
  public void startPipeline(PipelineConfiguration pipeline) {
    getStatsInfo().startPipeline(pipeline);
  }

  @Override
  public void stopPipeline(PipelineConfiguration pipeline) {
    getStatsInfo().stopPipeline(pipeline);
  }

  @Override
  public void incrementRecordCount(long count) {
    getStatsInfo().incrementRecordCount(count);
  }

  public ScheduledFuture getFuture() {
    return future;
  }
}
