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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.SysInfo;
import com.streamsets.lib.security.http.RestClient;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbstractStatsCollectorTask extends AbstractTask implements StatsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStatsCollectorTask.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.get();

  public static final String GET_TELEMETRY_URL_ENDPOINT = "usage.reporting.getTelemetryUrlEndpoint";
  /** Override to collect telemetry even for -SNAPSHOT versioned builds **/
  public static final String TELEMETRY_FOR_SNAPSHOT_BUILDS = "usage.reporting.getTelemetryUrlEndpoint.collectSnapshotBuilds";
  public static final String GET_TELEMETRY_URL_ENDPOINT_DEFAULT = "https://telemetry.streamsets.com/getTelemetryUrl";
  /** Tells getTelemetryUrl endpoint to use the test bucket **/
  public static final String TELEMETRY_USE_TEST_BUCKET = "usage.reporting.getTelemetryUrlEndpoint.useTestBucket";
  public static final boolean TELEMETRY_USE_TEST_BUCKET_DEFAULT = false;

  public static final String TELEMETRY_REPORT_PERIOD_SECONDS = "usage.reporting.period.seconds";

  // How often we report stats intervals, every 24 hrs
  public static final int TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT = 60 * 60 * 24;


  @VisibleForTesting
  static final String GET_TELEMETRY_URL_ARG_CLIENT_ID = "client_id";
  @VisibleForTesting
  static final String GET_TELEMETRY_URL_ARG_EXTENSION = "extension";
  @VisibleForTesting
  static final String GET_TELEMETRY_URL_ARG_EXTENSION_JSON = "json";
  @VisibleForTesting
  static final String GET_TELEMETRY_URL_ARG_TEST_BUCKET = "test_bucket";
  @VisibleForTesting
  static final String TELEMETRY_URL_KEY = "url";

  public static final String USAGE_PATH = "usage.reporting.path";
  public static final String USAGE_PATH_DEFAULT = "/public-rest/v4/usage/datacollector3";

  static final String TEST_ROLL_PERIOD_CONFIG = "stats.rollFrequency.test.minutes";

  static final String ROLL_PERIOD_CONFIG = "stats.rollFrequency.hours";
  static final long ROLL_PERIOD_CONFIG_MAX = 1;

  /**
   * Maximum number of minutes to wait between reporting if there are errors in reporting
   */
  private static final long REPORT_STATS_MAX_BACK_OFF_MINUTES = TimeUnit.DAYS.toMinutes(1);

  static final String OPT_FILE = "opt-stats.json";

  static final String STATS_FILE = "stats.json";

  static final String STATS_ACTIVE_KEY = "stats.active";
  static final String STATS_LAST_REPORT_KEY = "stats.lastReport";

  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final Configuration config;
  private final long rollFrequencyMillis;
  private final SafeScheduledExecutorService executorService;
  private final File optFile;
  private final File statsFile;
  private final SysInfo sysInfo;
  private final Activation activation;
  private boolean opted;
  private volatile boolean active;
  private long lastReport;
  private ScheduledFuture future;
  private volatile StatsInfo statsInfo;
  private int reportStatsBackOffCount;
  private long reportStatsBackOffUntil;

  /**
   * @deprecated use constructor with activation instead
   */
  @Deprecated
  public AbstractStatsCollectorTask(
          BuildInfo buildInfo,
          RuntimeInfo runtimeInfo,
          Configuration config,
          SafeScheduledExecutorService executorService,
          SysInfo sysInfo
  ) {
    this(buildInfo, runtimeInfo, config, executorService, sysInfo, null);
  }

  public AbstractStatsCollectorTask(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration config,
      SafeScheduledExecutorService executorService,
      SysInfo sysInfo,
      Activation activation
  ) {
    super("StatsCollector");
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.config = config;
    long rollFrequencyConfigMillis = (config.get(TEST_ROLL_PERIOD_CONFIG, -1) <= 0)?
        TimeUnit.HOURS.toMillis(config.get(ROLL_PERIOD_CONFIG, ROLL_PERIOD_CONFIG_MAX)) :
        TimeUnit.MINUTES.toMillis(config.get(TEST_ROLL_PERIOD_CONFIG, 1));
    rollFrequencyMillis = Math.min(TimeUnit.HOURS.toMillis(ROLL_PERIOD_CONFIG_MAX), rollFrequencyConfigMillis);
    this.executorService = executorService;
    optFile = new File(runtimeInfo.getDataDir(), OPT_FILE);
    statsFile = new File(runtimeInfo.getDataDir(), STATS_FILE);
    reportStatsBackOffCount = 0;
    reportStatsBackOffUntil = 0;
    this.sysInfo = sysInfo;
    this.activation = activation;
  }

  /**
   * Provide stats extensions. These instances will not be "live", but are snapshotted. To get the instances that are
   * actually used, call {@link #getStatsExtensions()}. Called on init and every roll.
   */
  abstract protected List<AbstractStatsExtension> provideStatsExtensions();

  /**
   * @return Live instances of stats extensions, which can be snapshotted, rolled, etc. as part of ActiveStats. Any
   * modifications to these objects must use {@link AbstractStatsExtension#doWithStatsInfoStructuralLock(Runnable)}.
   */
  protected final List<AbstractStatsExtension> getStatsExtensions() {
    return statsInfo.getActiveStats().getExtensions();
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
  protected File getOptFile() {
    return optFile;
  }

  @VisibleForTesting
  protected File getStatsFile() {
    return statsFile;
  }

  protected SysInfo getSysInfo() { return sysInfo; }

  protected Activation getActivation() { return activation; }

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

    statsInfo =  new StatsInfo(provideStatsExtensions());
    statsInfo.setCurrentSystemInfo(getBuildInfo(), getRuntimeInfo(), getSysInfo(), getActivation());

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
            if (isActive()) {
              if (map.containsKey(STATS_LAST_REPORT_KEY)) {
                lastReport = (Long) map.get(STATS_LAST_REPORT_KEY);
              }
            }
          }
        } catch (IOException ex) {
          opted = false;
          active = false;
          LOG.warn("Stats collection opt-in error, switching off and re-opting. Error: {}", ex.getMessage(), ex);
        }
      }
      if (isActive()) {
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
                // deserialization doesn't populate this back reference, so do it ourselves
                statsInfo.getActiveStats().getExtensions().forEach(e -> e.setStatsInfo(statsInfo));
                LOG.debug("Stats collection loaded");
              }
            }
          } catch (IOException ex) {
            opted = false;
            active = false;
            LOG.warn("Stats collection data is invalid, switching off and re-opting. Error: {}", ex.getMessage(), ex);
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
          LOG.error(
              "Could not delete opt-in status file. Stats Collection is disabled. Error: {}",
              ex.getMessage(),
              ex
          );
        }
      }
      if (!isActive()) {
        try {
          if (statsFile.exists()) {
            if (statsFile.delete()) {
              LOG.error("Could not delete stats collected data file. Stats Collection is disabled.");
            }
          }
        } catch (Exception ex) {
          LOG.error(
              "Could not delete stats collected data file. Stats Collection is disabled. Error: {}",
              ex.getMessage(),
              ex
          );
        }
      }
    }
    if (!getRuntimeInfo().isClusterSlave()) {
      LOG.info("Stats Collection, opted '{}, active '{}'", opted, isActive());
    }
    // when disabled all persistency/reporting done by the Runnable is a No Op.
    getRunnable(true).run();
    // report all old stuff before we trigger startSystem
    getStatsInfo().startSystem();
    future = executorService.scheduleAtFixedRate(getRunnable(false), 60, 60 , TimeUnit.SECONDS);
  }

  @VisibleForTesting
  StatsInfo updateAfterRoll(StatsInfo statsInfo) {
    // inject any new extensions
    Map<String, AbstractStatsExtension> existingToProcess = statsInfo.getActiveStats().getExtensions().stream()
        .collect(Collectors.toMap(
            e -> e.getClass().getName(),
            e -> e,
            (x, y) -> { throw new RuntimeException("No merge expected"); }));
    ImmutableList.Builder<AbstractStatsExtension> updatedStats = ImmutableList.builder();
    provideStatsExtensions().forEach(e -> {
      AbstractStatsExtension existing = existingToProcess.remove(e.getClass().getName());
      if (existing == null) {
        updatedStats.add(e.snapshot());
        LOG.info("Added stats extension: {}", e.getName());
      } else {
        updatedStats.add(existing);
        if (!e.getVersion().equals(existing.getVersion())) {
          LOG.warn("Stats extension {} not upgraded. Version is {} but expected {}.",
              e.getName(), existing.getVersion(), e.getVersion());
        }
      }
    });
    existingToProcess.values().forEach(e -> {
      LOG.warn("Removing stats extension {} of version {}", e.getName(), e.getVersion());
    });
    statsInfo.getActiveStats().setExtensions(updatedStats.build());
    statsInfo.getActiveStats().getExtensions().forEach(e -> e.setStatsInfo(statsInfo));
    return statsInfo;
  }

  long getReportPeriodSeconds() {
    return Math.min(
        TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT,
        config.get(TELEMETRY_REPORT_PERIOD_SECONDS, TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT)
    );
  }

  Runnable getRunnable(boolean forceRollAndReport) {
    return () -> {
      synchronized (AbstractStatsCollectorTask.this) {
        /*
        Roll every 1 hr, or on initial run
        Report every 24 hrs, on initial run
        Save stats every 60s (protects against crashes)
        On graceful shutdown, this is run one last time as well (mostly to guarantee save stats)
        */
        if (isActive()) {
          if (getStatsInfo().rollIfNeeded(getBuildInfo(), getRuntimeInfo(), getSysInfo(),
                  getActivation(), getRollFrequencyMillis(),
                  forceRollAndReport, getCurrentTimeMillis())) {
            updateAfterRoll(getStatsInfo());
            LOG.debug("Stats collection data rolled");
          }
          if (shouldReport(forceRollAndReport)) {
            LOG.debug("Reporting");

            //ignoring stats already reported (we keep them for longer per SDC-14937
            List<StatsBean> statsToReport = getStatsInfo().getCollectedStats()
                .stream()
                .filter(s -> !s.isReported())
                .collect(Collectors.toList());
            if (reportStats(statsToReport)) {
              LOG.debug("Reported");
              reportStatsBackOffCount = 0;
              reportStatsBackOffUntil = 0;
            } else {
              reportStatsBackOffCount++;
              LOG.debug("Reporting has failed {} times in a row (with exponential back off).", reportStatsBackOffCount);
              // careful not to shift too many bits or it wraps around
              long backOffMinutes = Math.min(
                      REPORT_STATS_MAX_BACK_OFF_MINUTES,
                      1L << Math.min(
                              30,
                              reportStatsBackOffCount));
              LOG.warn("Due to reporting failures, reporting will back off for {} minutes", backOffMinutes);
              reportStatsBackOffUntil = getCurrentTimeMillis() + TimeUnit.MINUTES.toMillis(backOffMinutes);
            }
          }
          saveStatsInternal();
        }
      }
    };
  }

  /**
   * Mockable method to get current time in millis
   * @return
   */
  @VisibleForTesting
  long getCurrentTimeMillis() {
    return System.currentTimeMillis();
  }

  private boolean shouldReport(boolean forceNextReport) {
    if (getStatsInfo().getCollectedStats().isEmpty()) {
      return false;
    }
    StatsBean firstNotReported = getStatsInfo().getCollectedStats().stream().filter(s -> ! s.isReported()).findFirst().orElse(null);
    if (firstNotReported == null) { // nothing to report
      return false;
    }
    if (forceNextReport) {
      LOG.debug("shouldReport because next report is forced");
      return true;
    }
    long now = getCurrentTimeMillis();
    if (reportStatsBackOffUntil > 0) {
      if (reportStatsBackOffUntil > now) {
        LOG.debug("shouldReport false until now ({}) >= reportStatsBackOffUntil({})", now, reportStatsBackOffUntil);
        return false;
      } else {
        LOG.debug("shouldReport true since we need to retry a failure, now ({}) >= reportStatsBackOffUntil({})",
                now, reportStatsBackOffUntil);
        return true;
      }
    }
    // at this point we know there is non reported interval
    long oldestStartTime = firstNotReported.getStartTime();
    long delta = now - oldestStartTime;
    boolean report = delta > (TimeUnit.SECONDS.toMillis(getReportPeriodSeconds()) * 0.99);
    LOG.debug("shouldReport returning {} with oldest start time {} delta {}", report, oldestStartTime, delta);
    return report;
  }

  protected boolean reportStats(List<StatsBean> stats) {
    return reportStats(stats, null);
  }

  /**
   *
   * @param stats Normal uploads should provide this list. Mutually exclusive with rawStats.
   * @param rawStats When provided, then will simply report these raw stats without updating any state. Used for
   *                 uploading air-gapped stats. Must be an API stats object, StatsInfo, or a JSON array of StatsBeans.
   * @return
   */
  protected boolean reportStats(List<StatsBean> stats, String rawStats) {
    Preconditions.checkArgument(stats == null ^ rawStats == null,
        "Must provide exactly one of the two arguments");

    String sdcId;
    String rawStatsToUpload = null;
    if (stats != null) {
      sdcId = getRuntimeInfo().getId();
    } else {
      try {
        Object deserializedStats = OBJECT_MAPPER.readValue(rawStats, Object.class);
        List<Map<String, Object>> rawEntries;
        if (deserializedStats instanceof List) {
          rawEntries = (List<Map<String, Object>>) deserializedStats;
          rawStatsToUpload = rawStats;
        } else {
          Map<String, Object> rawStatsInfo = (Map<String, Object>) deserializedStats;
          if (!rawStatsInfo.containsKey("collectedStats")) {
            if (!rawStatsInfo.containsKey("stats")) {
              throw new IllegalArgumentException("No stats provided. Make sure Usage Statistics is enabled, and the "
                  + "argument is an API stats object, StatsInfo (stats.json), or a JSON array of StateBeans.");
            }
            // this is an API /system/stats object, unwrap the StatsInfo
            rawStatsInfo = (Map<String,Object>) rawStatsInfo.get("stats");
          }
          Preconditions.checkArgument(rawStatsInfo.containsKey("collectedStats"),
              "Argument must be an API stats object, StatsInfo (stats.json), or a JSON array of StateBeans.");
          rawEntries = (List<Map<String, Object>>) rawStatsInfo.get("collectedStats");
          rawStatsToUpload = OBJECT_MAPPER.writeValueAsString(rawEntries);
        }
        if (rawEntries.isEmpty()) {
          // nothing to report
          return true;
        }
        Preconditions.checkArgument(rawEntries.get(0).containsKey("sdcId"), "stats must have an sdcId");
        sdcId = (String) rawEntries.get(0).get("sdcId");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    boolean reported = false;
    String getTelemetryUrlEndpoint = config.get(GET_TELEMETRY_URL_ENDPOINT, GET_TELEMETRY_URL_ENDPOINT_DEFAULT);
    if (isTelemetryEnabled(getTelemetryUrlEndpoint)) {
      try {
        // RestClient adds a trailing slash to the "baseUrl", so we have to split it up just to avoid that
        URL getTelemetryUrl = new URL(getTelemetryUrlEndpoint);
        String baseUrl = getTelemetryUrlEndpoint.substring(
            0,
            getTelemetryUrlEndpoint.length() - getTelemetryUrl.getPath().length());
        RestClient client = RestClient.builder(baseUrl)
            .json(true)
            .name(sdcId)
            .path(getTelemetryUrl.getPath())
            .build();
        ImmutableMap.Builder<String, String> argsMapBuilder = ImmutableMap.builder();
        argsMapBuilder.put(GET_TELEMETRY_URL_ARG_CLIENT_ID, sdcId);
        argsMapBuilder.put(GET_TELEMETRY_URL_ARG_EXTENSION, GET_TELEMETRY_URL_ARG_EXTENSION_JSON);
        if (config.get(TELEMETRY_USE_TEST_BUCKET, TELEMETRY_USE_TEST_BUCKET_DEFAULT)) {
          argsMapBuilder.put(GET_TELEMETRY_URL_ARG_TEST_BUCKET, "True"); // any non-empty string is treated as true
        }
        RestClient.Response response = postToGetTelemetryUrl(client, argsMapBuilder.build());
        if (response.successful()) {
          Map<String, String> responseData = response.getData(new TypeReference<Map<String, String>>(){});
          String uploadUrlString = responseData.get(TELEMETRY_URL_KEY);
          if (null != uploadUrlString) {
            reported = uploadToUrl(stats, rawStatsToUpload, uploadUrlString);

            if (reported && stats != null) {
              // setting stats as reported
              stats.forEach(s -> s.setReported());
            }
          } else {
            LOG.warn("Unable to get telemetry URL from endpoint {}, url missing from responseData {}",
                getTelemetryUrlEndpoint,
                responseData);
          }
        } else {
          LOG.warn("Unable to get telemetry URL from endpoint {}, response code: {}",
              getTelemetryUrlEndpoint,
              response.getStatus());
        }
      } catch (Exception ex) {
        LOG.warn("Usage reporting failed. Error: {}", ex.getMessage(), ex);
      }
    } else {
      LOG.debug("Reporting disabled");
      reported = true;
    }
    return reported;
  }

  private boolean isTelemetryEnabled(String getTelemetryUrlEndpoint) {
    if (!getTelemetryUrlEndpoint.startsWith("http")) {
      // not configured with valid url
      return false;
    }
    if (buildInfo.getVersion().endsWith("-SNAPSHOT") && !config.get(TELEMETRY_FOR_SNAPSHOT_BUILDS, false)) {
      // don't collect for development builds
      return false;
    }
    return true;
  }

  // This is mostly here for mocking in tests
  @VisibleForTesting
  protected RestClient.Response postToGetTelemetryUrl(RestClient client, Object data) throws IOException {
    return client.post(data);
  }

  private boolean uploadToUrl(List<StatsBean> stats, String rawStats, String uploadUrlString) throws IOException {
    Preconditions.checkArgument(stats == null ^ rawStats == null,
        "Must provide exactly one of the two arguments for stats");
    boolean reported = false;
    if (stats != null) {
      LOG.debug("Uploading {} stats to url {}", stats.size(), uploadUrlString);
    } else {
      LOG.debug("Uploading raw stats of size {} to url {}", rawStats.length(), uploadUrlString);
    }
    URL uploadUrl = new URL(uploadUrlString);
    // Avoid RestClient since sometimes extra headers cause a problem with signed URLs
    HttpURLConnection connection = getHttpURLConnection(uploadUrl);
    connection.setDoInput(true);
    connection.setDoOutput(true);
    connection.setRequestMethod("PUT");
    OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
    if (stats != null) {
      OBJECT_MAPPER.writeValue(out, stats);
    } else {
      out.write(rawStats);
    }
    out.close();
    int responseCode = connection.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      reported = true;
    } else {
      LOG.warn("Failed to upload to url {} with code {} and message: {} and stream: {}",
          uploadUrlString,
          responseCode,
          connection.getResponseMessage(),
          IOUtils.toString(connection.getInputStream()));
      connection.getInputStream().close();
    }
    return reported;
  }

  // This is just so it can be mocked
  @VisibleForTesting
  protected HttpURLConnection getHttpURLConnection(URL uploadUrl) throws IOException {
    return (HttpURLConnection) uploadUrl.openConnection();
  }

  protected void saveStatsInternal() {
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
      LOG.warn("Could not save stats collection, Disabling and re-opting. Error: {}", ex.getMessage(), ex);
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
    getRunnable(true).run();
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
            ImmutableMap.of(STATS_ACTIVE_KEY, active, STATS_LAST_REPORT_KEY, getCurrentTimeMillis())
        );
        this.active = active;
        opted = true;
      } catch (IOException ex) {
        this.active = false;
        opted = false;
        LOG.warn("Could not change stats collection state, Disabling and re-opting. Error: {}", ex.getMessage(), ex);
      }
      getStatsInfo().reset();
      if (active) {
        // Active flag changed to active, trigger roll and report
        // This internally save stats as well
        getRunnable(true).run();
      } else {
        saveStatsInternal();
      }
    }
  }

  @Override
  public void saveStats() {
    getRunnable(false).run();
  }

  @Override
  public void createPipeline(String pipelineId) {
    getStatsInfo().createPipeline(pipelineId);
  }

  @Override
  public void previewPipeline(String pipelineId) {
    getStatsInfo().previewPipeline(pipelineId);
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
  public void errorCode(ErrorCode errorCode) {
    getStatsInfo().errorCode(errorCode);
  }

  @Override
  public void incrementRecordCount(long count) {
    getStatsInfo().incrementRecordCount(count);
  }

  @Override
  public void previewStatusChanged(PreviewStatus previewStatus, Previewer previewer) {
    getStatsInfo().previewStatusChanged(previewStatus, previewer);
  }

  @Override
  public void pipelineStatusChanged(PipelineStatus pipelineStatus, PipelineConfiguration conf, Pipeline pipeline) {
    getStatsInfo().pipelineStatusChanged(pipelineStatus, conf, pipeline);
  }

  public ScheduledFuture getFuture() {
    return future;
  }
}
