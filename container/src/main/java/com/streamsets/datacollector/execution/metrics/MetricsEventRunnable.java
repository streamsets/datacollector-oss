/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.execution.metrics;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.event.client.impl.MovedDpmJerseyClientFilter;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.runner.cluster.SlaveCallbackManager;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.http.SnappyWriterInterceptor;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.event.json.CounterJson;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class MetricsEventRunnable implements Runnable {

  public static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;
  private static final String DPM_PIPELINE_COMMIT_ID = "dpm.pipeline.commitId";
  private static final String DPM_JOB_ID = "dpm.job.id";
  private static final String REMOTE_TIMESERIES_URL = "REMOTE_TIMESERIES_URL";
  private static final String PIPELINE_COMMIT_ID = "PIPELINE_COMMIT_ID";
  private static final String JOB_ID = "JOB_ID";
  private static final String UPDATE_WAIT_TIME_MS = "UPDATE_WAIT_TIME_MS";
  public static final String TIME_SERIES_ANALYSIS = "TIME_SERIES_ANALYSIS";
  public static final String CONTROL_HUB_METRICS_URL = "timeseries/rest/v1/metrics";
  public static final String RUNNABLE_NAME = "MetricsEventRunnable";
  private static final Logger LOG = LoggerFactory.getLogger(MetricsEventRunnable.class);
  private final ConcurrentMap<String, MetricRegistryJson> slaveMetrics;
  private ThreadHealthReporter threadHealthReporter;
  private final EventListenerManager eventListenerManager;
  private final SlaveCallbackManager slaveCallbackManager;
  private final PipelineStateStore pipelineStateStore;
  private final MetricRegistry metricRegistry;
  private final String name;
  private final String rev;
  private final int scheduledDelay;
  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;
  private BlockingQueue<Record> statsQueue;
  private PipelineConfiguration pipelineConfiguration;
  private MetricRegistryJson metricRegistryJson;

  private boolean isDPMPipeline = false;
  private String pipelineCommitId;
  private String jobId;
  private Integer waitTimeBetweenUpdates;
  private final int retryAttempts = 5;
  private boolean timeSeriesAnalysis = true;
  private volatile boolean isPipelineStopped = false;
  private Stopwatch stopwatch = null;

  @Inject
  public MetricsEventRunnable(
      @Named("name") String name,
      @Named("rev") String rev,
      Configuration configuration,
      PipelineStateStore pipelineStateStore,
      ThreadHealthReporter threadHealthReporter,
      EventListenerManager eventListenerManager,
      MetricRegistry metricRegistry,
      SlaveCallbackManager slaveCallbackManager,
      RuntimeInfo runtimeInfo
  ) {
    slaveMetrics = new ConcurrentHashMap<>();
    this.threadHealthReporter = threadHealthReporter;
    this.eventListenerManager = eventListenerManager;
    this.slaveCallbackManager = slaveCallbackManager;
    this.pipelineStateStore = pipelineStateStore;
    this.metricRegistry = metricRegistry;
    this.name = name;
    this.rev = rev;
    this.scheduledDelay = configuration.get(REFRESH_INTERVAL_PROPERTY, REFRESH_INTERVAL_PROPERTY_DEFAULT);
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;

    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  public void onStopOrFinishPipeline() {
    this.threadHealthReporter = null;
    if (isDPMPipeline) {
      // Send final metrics to Control Hub on stop
      sendMetricsInternal(true);
    }
  }

  public void setStatsQueue(BlockingQueue<Record> statsQueue) {
    this.statsQueue = statsQueue;
  }

  public void setPipelineConfiguration(PipelineConfiguration pipelineConfiguration) {
    this.pipelineConfiguration = pipelineConfiguration;
    this.initializeDPMMetricsVariables();
  }

  public void setMetricRegistryJson(MetricRegistryJson metricRegistryJson) {
    this.metricRegistryJson = metricRegistryJson;
  }

  private void sendMetricsInternal(boolean onPipelineStop) {
    if (LOG.isTraceEnabled()) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      LOG.trace("MetricsEventRunnable Run - {}, Is pipeline stopped - {}", sdf.format(new Date()), onPipelineStop);
    }
    if (!isPipelineStopped) {
      synchronized (this) {
        if (!isPipelineStopped) {
          if (onPipelineStop) {
            this.stopwatch = null;
            this.isPipelineStopped = true;
          }
          doRun();
        }
      }
    }
  }

  @Override
  public void run() {
    sendMetricsInternal(false);
  }


  private void doRun() {
    try {
      if(threadHealthReporter != null) {
        threadHealthReporter.reportHealth(RUNNABLE_NAME, scheduledDelay, System.currentTimeMillis());
      }
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      PipelineState state = pipelineStateStore.getState(name, rev);
      if (hasMetricEventListeners(state) ||
          (isDPMPipeline && (isWriteStatsToDPMDirectlyEnabled() || isStatAggregationEnabled()))) {
        // compute aggregated metrics in case of cluster mode pipeline
        // get individual pipeline metrics if non cluster mode pipeline
        String metricsJSONStr;
        if (state.getExecutionMode() == ExecutionMode.CLUSTER_BATCH
          || state.getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
          || state.getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING) {
          MetricRegistryJson json = getAggregatedMetrics();
          metricsJSONStr = objectMapper.writer().writeValueAsString(json);
        } else if ((state.getExecutionMode() == ExecutionMode.BATCH
            || state.getExecutionMode() == ExecutionMode.STREAMING) && metricRegistryJson != null) {
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistryJson);
        } else {
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistry);
        }
        if (hasMetricEventListeners(state)) {
          eventListenerManager.broadcastMetrics(name, metricsJSONStr);
        }
        // don't queue stats record when pipeline is stopped as runner is not going to process any more batches
        if (isStatAggregationEnabled() && !isPipelineStopped) {
          AggregatorUtil.enqueStatsRecord(
            AggregatorUtil.createMetricJsonRecord(
                runtimeInfo.getId(),
                runtimeInfo.getMasterSDCId(),
                pipelineConfiguration.getMetadata(),
                false, // isAggregated - no its not aggregated
                timeSeriesAnalysis,
                false,
                metricsJSONStr
            ),
            statsQueue,
            configuration
          );
        } else if (isDPMPipeline && isWriteStatsToDPMDirectlyEnabled() &&
            state.getExecutionMode() != ExecutionMode.SLAVE) {
          // Write Stats to Control hub is not supported for slave nodes
          sendMetricsToDPM(pipelineConfiguration, metricsJSONStr);
        }
      }
    } catch (IOException ex) {
      LOG.warn("Error while serializing metrics, {}", ex.toString(), ex);
    } catch (PipelineStoreException ex) {
      LOG.warn("Error while fetching status of pipeline,  {}", ex.toString(), ex);
    }
  }

  public MetricRegistryJson getAggregatedMetrics() {
    MetricRegistryJson aggregatedMetrics = new MetricRegistryJson();
    Map<String, CounterJson> aggregatedCounters = new HashMap<>();
    Map<String, MeterJson> aggregatedMeters = new HashMap<>();
    List<String> slaves = new ArrayList<>();

    for(CallbackInfo callbackInfo : slaveCallbackManager.getSlaveCallbackList(CallbackObjectType.METRICS)) {
      slaves.add(callbackInfo.getSdcURL());
      MetricRegistryJson metricRegistryJson = callbackInfo.getCallbackInfoHelper().getMetricRegistryJson();
      if(metricRegistryJson != null) {
        slaveMetrics.put(callbackInfo.getSdcSlaveToken(), metricRegistryJson);
      }
    }

    for(Map.Entry<String, MetricRegistryJson> entry: slaveMetrics.entrySet()) {
      MetricRegistryJson metrics = entry.getValue();

      Map<String, CounterJson> slaveCounters = metrics.getCounters();
      Map<String, MeterJson> slaveMeters = metrics.getMeters();

      for (Map.Entry<String, CounterJson> counterJsonEntry : slaveCounters.entrySet()) {
        CounterJson slaveCounter = counterJsonEntry.getValue();
        CounterJson aggregatedCounter = aggregatedCounters.getOrDefault(counterJsonEntry.getKey(), new CounterJson());
        aggregatedCounter.setCount(aggregatedCounter.getCount() + slaveCounter.getCount());
        aggregatedCounters.put(counterJsonEntry.getKey(), aggregatedCounter);
      }

      for (Map.Entry<String, MeterJson> meterJsonEntry : slaveMeters.entrySet()) {
        MeterJson slaveMeter = meterJsonEntry.getValue();
        MeterJson aggregatedMeter = aggregatedMeters.getOrDefault(meterJsonEntry.getKey(), new MeterJson());
        aggregatedMeter.setCount(aggregatedMeter.getCount() + slaveMeter.getCount());
        aggregatedMeter.setM1_rate(aggregatedMeter.getM1_rate() + slaveMeter.getM1_rate());
        aggregatedMeter.setM5_rate(aggregatedMeter.getM5_rate() + slaveMeter.getM5_rate());
        aggregatedMeter.setM15_rate(aggregatedMeter.getM15_rate() + slaveMeter.getM15_rate());
        aggregatedMeter.setM30_rate(aggregatedMeter.getM30_rate() + slaveMeter.getM30_rate());
        aggregatedMeter.setH1_rate(aggregatedMeter.getH1_rate() + slaveMeter.getH1_rate());
        aggregatedMeter.setH6_rate(aggregatedMeter.getH6_rate() + slaveMeter.getH6_rate());
        aggregatedMeter.setH12_rate(aggregatedMeter.getH12_rate() + slaveMeter.getH12_rate());
        aggregatedMeter.setH24_rate(aggregatedMeter.getH24_rate() + slaveMeter.getH24_rate());
        aggregatedMeter.setMean_rate(aggregatedMeter.getMean_rate() + slaveMeter.getMean_rate());
        aggregatedMeters.put(meterJsonEntry.getKey(), aggregatedMeter);
      }
    }

    aggregatedMetrics.setCounters(aggregatedCounters);
    aggregatedMetrics.setMeters(aggregatedMeters);
    aggregatedMetrics.setSlaves(slaves);

    return aggregatedMetrics;
  }

  public int getScheduledDelay() {
    return scheduledDelay;
  }

  public void clearSlaveMetrics() {
    this.slaveMetrics.clear();
  }

  private boolean isStatAggregationEnabled() {
    return null != statsQueue;
  }

  private boolean isWriteStatsToDPMDirectlyEnabled() {
    boolean isEnabled = false;
    StageConfiguration statsAggregatorStage = pipelineConfiguration.getStatsAggregatorStage();
    if (statsAggregatorStage == null ||
        statsAggregatorStage.getStageName().equals(StandaloneRunner.STATS_DPM_DIRECTLY_TARGET)) {
      isEnabled = true;
    }
    return isEnabled;
  }

  private boolean hasMetricEventListeners(PipelineState state) {
    return eventListenerManager.hasMetricEventListeners(name) && state.getStatus().isActive();
  }

  protected boolean isRemotePipeline(PipelineState pipelineState) {
    Object isRemote = pipelineState.getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    return isRemote != null && (boolean) isRemote;
  }

  private void initializeDPMMetricsVariables() {
    try {
      PipelineState state = pipelineStateStore.getState(name, rev);
      isDPMPipeline = isRemotePipeline(state);
      if (isDPMPipeline && isWriteStatsToDPMDirectlyEnabled()) {
        PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get()
            .create(pipelineConfiguration, new ArrayList<>(), null, null, null);
        for (String key : pipelineConfigBean.constants.keySet()) {
          switch (key) {
            case PIPELINE_COMMIT_ID:
              pipelineCommitId = (String) pipelineConfigBean.constants.get(key);
              break;
            case JOB_ID:
              jobId = (String) pipelineConfigBean.constants.get(key);
              break;
            case UPDATE_WAIT_TIME_MS:
              if (pipelineConfigBean.constants.get(key) != null) {
                waitTimeBetweenUpdates = (Integer) pipelineConfigBean.constants.get(key);
              } else {
                waitTimeBetweenUpdates = 15000;
              }
              break;
            case TIME_SERIES_ANALYSIS:
              if (pipelineConfigBean.constants.get(key) != null) {
                timeSeriesAnalysis = (Boolean) pipelineConfigBean.constants.get(key);
              } else {
                timeSeriesAnalysis = true;
              }
              break;
          }
        }
      }
    } catch (PipelineStoreException e) {
      LOG.error(Utils.format("Error when reading pipeline state: {}", e.getErrorMessage(), e));
    }
  }

  private void sendMetricsToDPM(
      PipelineConfiguration pipelineConfiguration,
      String metricsJSONStr
  ) throws IOException {
    if (stopwatch == null || stopwatch.elapsed(TimeUnit.MILLISECONDS) > waitTimeBetweenUpdates || isPipelineStopped) {
      SDCMetricsJson sdcMetricsJson = new SDCMetricsJson();
      sdcMetricsJson.setTimestamp(System.currentTimeMillis());
      sdcMetricsJson.setAggregated(false);
      sdcMetricsJson.setSdcId(runtimeInfo.getId());
      sdcMetricsJson.setMasterSdcId(runtimeInfo.getMasterSDCId());
      if (metricRegistryJson != null) {
        sdcMetricsJson.setMetrics(metricRegistryJson);
      } else {
        sdcMetricsJson.setMetrics(ObjectMapperFactory.get().readValue(metricsJSONStr, MetricRegistryJson.class));
      }
      Map<String, String> metadata = new HashMap<>();
      if (pipelineConfiguration.getMetadata() != null && !pipelineConfiguration.getMetadata().isEmpty()) {
        for (Map.Entry<String, Object> e : pipelineConfiguration.getMetadata().entrySet()) {
          if (e.getValue() instanceof String) {
            metadata.put(e.getKey(), (String) e.getValue());
          }
        }
      }
      metadata.put(DPM_PIPELINE_COMMIT_ID, pipelineCommitId);
      metadata.put(DPM_JOB_ID, jobId);
      metadata.put(AggregatorUtil.TIME_SERIES_ANALYSIS, String.valueOf(timeSeriesAnalysis));
      sdcMetricsJson.setMetadata(metadata);

      if (sdcMetricsJson.getMetrics() != null) {
        sendUpdate(ImmutableList.of(sdcMetricsJson));
      }

      if (stopwatch == null) {
        stopwatch = Stopwatch.createStarted();
      } else {
        stopwatch.reset()
            .start();
      }
    }
  }

  private void sendUpdate(List<SDCMetricsJson> sdcMetricsJsonList) {
    int delaySecs = 1;
    int attempts = 0;
    while (attempts < retryAttempts || retryAttempts == -1) {
      if (attempts > 0) {
        delaySecs = delaySecs * 2;
        delaySecs = Math.min(delaySecs, 60);
        LOG.warn("Post attempt '{}', waiting for '{}' seconds before retrying ...",
            attempts, delaySecs);
        sleep(delaySecs);
      }
      attempts++;
      Response response = null;
      try {
        DpmClientInfo clientInfo = runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(new MovedDpmJerseyClientFilter(clientInfo));
        clientConfig.register(SnappyWriterInterceptor.class);
        String remoteTimeSeriesUrl = clientInfo.getDpmBaseUrl() + CONTROL_HUB_METRICS_URL;
        Client client = ClientBuilder.newBuilder().newClient(clientConfig);
        WebTarget webTarget = client.target(remoteTimeSeriesUrl);

        Invocation.Builder invocationBuilder = webTarget.request().header(
            SSOConstants.X_REST_CALL,
            SSOConstants.SDC_COMPONENT_NAME
        );
        invocationBuilder.header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
        clientInfo.getHeaders().entrySet().forEach(e -> invocationBuilder.header(e.getKey(), e.getValue()));
        response = invocationBuilder.post(Entity.json(sdcMetricsJsonList));
        if (response.getStatus() == HttpURLConnection.HTTP_OK) {
          LOG.trace("Sending metrics was successful");
          return;
        } else if (response.getStatus() == HttpURLConnection.HTTP_UNAVAILABLE) {
          LOG.warn("Error writing to time-series app: Control Hub unavailable");
          // retry
        } else if (response.getStatus() == HttpURLConnection.HTTP_FORBIDDEN) {
          // no retry in this case
          String errorResponseMessage = response.readEntity(String.class);
          LOG.error(Utils.format("Error writing to Control Hub: {}", errorResponseMessage));
          return;
        } else {
          String responseMessage = response.readEntity(String.class);
          LOG.error(Utils.format("Error writing to Control Hub: {}", responseMessage));
          //retry
        }
      } catch (Exception ex) {
        LOG.error(Utils.format("Error writing to Control Hub: {}", ex.toString(), ex));
        // retry
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }

    // no success after retry
    LOG.warn("Unable to write metrics to Control Hub after {} attempts", retryAttempts);
  }

  public static void sleep(int secs) {
    try {
      Thread.sleep(secs * 1000);
    } catch (InterruptedException ex) {
      String msg = "Interrupted while attempting to fetch latest Metrics from Control Hub";
      LOG.error(msg);
      throw new RuntimeException(msg, ex);
    }
  }

}
