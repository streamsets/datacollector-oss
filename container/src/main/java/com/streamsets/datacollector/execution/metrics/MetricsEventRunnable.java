/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.runner.cluster.SlaveCallbackManager;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetricsEventRunnable implements Runnable {

  public static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;

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
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    this.threadHealthReporter = threadHealthReporter;
  }

  public void setStatsQueue(BlockingQueue<Record> statsQueue) {
    this.statsQueue = statsQueue;
  }

  public void setPipelineConfiguration(PipelineConfiguration pipelineConfiguration) {
    this.pipelineConfiguration = pipelineConfiguration;
  }

  @Override
  public void run() {
    //Added log trace to debug SDC-725
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Date now = new Date();
    LOG.trace("MetricsEventRunnable Run - " + sdf.format(now));
    try {
      if(threadHealthReporter != null) {
        threadHealthReporter.reportHealth(RUNNABLE_NAME, scheduledDelay, System.currentTimeMillis());
      }
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      PipelineState state = pipelineStateStore.getState(name, rev);
      if (hasMetricEventListeners(state) || isStatAggregationEnabled()) {
        // compute aggregated metrics in case of cluster mode pipeline
        // get individual pipeline metrics if non cluster mode pipeline
        String metricsJSONStr;
        if (state.getExecutionMode() == ExecutionMode.CLUSTER_BATCH
          || state.getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
          || state.getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING) {
          MetricRegistryJson metricRegistryJson = getAggregatedMetrics();
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistryJson);
        } else {
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistry);
        }
        if (hasMetricEventListeners(state)) {
          eventListenerManager.broadcastMetrics(name, metricsJSONStr);
        }
        if (isStatAggregationEnabled()) {
          AggregatorUtil.enqueStatsRecord(
            AggregatorUtil.createMetricJsonRecord(
                runtimeInfo.getId(),
                pipelineConfiguration.getMetadata(),
                false, // isAggregated - no its not aggregated
                metricsJSONStr
            ),
            statsQueue,
            configuration
          );
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
    Map<String, CounterJson> aggregatedCounters = null;
    Map<String, MeterJson> aggregatedMeters = null;
    List<String> slaves = new ArrayList<>();

    for(CallbackInfo callbackInfo : slaveCallbackManager.getSlaveCallbackList()) {
      slaves.add(callbackInfo.getSdcURL());
      MetricRegistryJson metricRegistryJson = callbackInfo.getMetricRegistryJson();
      if(metricRegistryJson != null) {
        slaveMetrics.put(callbackInfo.getSdcSlaveToken(), callbackInfo.getMetricRegistryJson());
      }
    }

    for(Map.Entry<String, MetricRegistryJson> entry: slaveMetrics.entrySet()) {
      MetricRegistryJson metrics = entry.getValue();

      Map<String, CounterJson> slaveCounters = metrics.getCounters();
      Map<String, MeterJson> slaveMeters = metrics.getMeters();

      if(aggregatedCounters == null) {
        //First Slave Metrics

        aggregatedCounters = new HashMap<>();
        aggregatedMeters = new HashMap<>();

        for(Map.Entry<String, CounterJson> counterJsonEntry: slaveCounters.entrySet()) {
          CounterJson slaveCounter = counterJsonEntry.getValue();
          CounterJson aggregatedCounter = new CounterJson();
          aggregatedCounter.setCount(slaveCounter.getCount());
          aggregatedCounters.put(counterJsonEntry.getKey(), aggregatedCounter);
        }

        for(Map.Entry<String, MeterJson> meterJsonEntry: slaveMeters.entrySet()) {
          MeterJson slaveMeter = meterJsonEntry.getValue();
          MeterJson aggregatedMeter = new MeterJson();
          aggregatedMeter.setCount( slaveMeter.getCount());
          aggregatedMeter.setM1_rate(slaveMeter.getM1_rate());
          aggregatedMeter.setM5_rate(slaveMeter.getM5_rate());
          aggregatedMeter.setM15_rate(slaveMeter.getM15_rate());
          aggregatedMeter.setM30_rate(slaveMeter.getM30_rate());
          aggregatedMeter.setH1_rate(slaveMeter.getH1_rate());
          aggregatedMeter.setH6_rate(slaveMeter.getH6_rate());
          aggregatedMeter.setH12_rate(slaveMeter.getH12_rate());
          aggregatedMeter.setH24_rate(slaveMeter.getH24_rate());
          aggregatedMeter.setMean_rate(slaveMeter.getMean_rate());
          aggregatedMeters.put(meterJsonEntry.getKey(), aggregatedMeter);
        }

      } else {
        //Otherwise add to the aggregated Metrics
        for(Map.Entry<String, CounterJson> counterJsonEntry : aggregatedCounters.entrySet()) {
          CounterJson aggregatedCounter = counterJsonEntry.getValue();
          CounterJson slaveCounter = slaveCounters.get(counterJsonEntry.getKey());

          if(aggregatedCounter != null && slaveCounter != null) {
            aggregatedCounter.setCount(aggregatedCounter.getCount() + slaveCounter.getCount());
          }
        }

        for(Map.Entry<String, MeterJson> meterJsonEntry : aggregatedMeters.entrySet()) {
          MeterJson aggregatedMeter = meterJsonEntry.getValue();
          MeterJson slaveMeter = slaveMeters.get(meterJsonEntry.getKey());

          if(aggregatedMeter != null && slaveMeter != null) {
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
          }
        }
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

  private boolean hasMetricEventListeners(PipelineState state) {
    return eventListenerManager.hasMetricEventListeners(name) && state.getStatus().isActive();
  }
}
