/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.runner.cluster.SlaveCallbackManager;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetricsEventRunnable implements Runnable {

  public static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;

  public static final String RUNNABLE_NAME = "MetricsEventRunnable";
  private final static Logger LOG = LoggerFactory.getLogger(MetricsEventRunnable.class);
  private final ConcurrentMap<String, MetricRegistryJson> slaveMetrics;
  private ThreadHealthReporter threadHealthReporter;
  private final EventListenerManager eventListenerManager;
  private final SlaveCallbackManager slaveCallbackManager;
  private final PipelineStateStore pipelineStateStore;
  private final MetricRegistry metricRegistry;
  private final String name;
  private final String rev;
  private final int scheduledDelay;

  @Inject
  public MetricsEventRunnable(@Named("name") String name, @Named("rev") String rev, Configuration configuration,
                              PipelineStateStore pipelineStateStore, ThreadHealthReporter threadHealthReporter,
                              EventListenerManager eventListenerManager, MetricRegistry metricRegistry,
                              SlaveCallbackManager slaveCallbackManager) {
    slaveMetrics = new ConcurrentHashMap<>();
    this.threadHealthReporter = threadHealthReporter;
    this.eventListenerManager = eventListenerManager;
    this.slaveCallbackManager = slaveCallbackManager;
    this.pipelineStateStore = pipelineStateStore;
    this.metricRegistry = metricRegistry;
    this.name = name;
    this.rev = rev;
    this.scheduledDelay = configuration.get(REFRESH_INTERVAL_PROPERTY, REFRESH_INTERVAL_PROPERTY_DEFAULT);
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    this.threadHealthReporter = threadHealthReporter;
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
      PipelineState state = pipelineStateStore.getState(name, rev);
      if (eventListenerManager.hasMetricEventListeners(name) && state.getStatus().isActive()) {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String metricsJSONStr;
        if (state.getExecutionMode() == ExecutionMode.CLUSTER_BATCH
          || state.getExecutionMode() == ExecutionMode.CLUSTER_STREAMING) {
          MetricRegistryJson metricRegistryJson = getAggregatedMetrics();
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistryJson);
        } else {
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistry);
        }
        eventListenerManager.broadcastMetrics(name, metricsJSONStr);
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

    for(String slaveSdcToken: slaveMetrics.keySet()) {
      MetricRegistryJson metrics = slaveMetrics.get(slaveSdcToken);

      Map<String, CounterJson> slaveCounters = metrics.getCounters();
      Map<String, MeterJson> slaveMeters = metrics.getMeters();

      if(aggregatedCounters == null) {
        //First Slave Metrics

        aggregatedCounters = new HashMap<>();
        aggregatedMeters = new HashMap<>();

        for(String meterName: slaveCounters.keySet()) {
          CounterJson slaveCounter = slaveCounters.get(meterName);
          CounterJson aggregatedCounter = new CounterJson();
          aggregatedCounter.setCount(slaveCounter.getCount());
          aggregatedCounters.put(meterName, aggregatedCounter);
        }

        for(String meterName: slaveMeters.keySet()) {
          MeterJson slaveMeter = slaveMeters.get(meterName);
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
          aggregatedMeters.put(meterName, aggregatedMeter);
        }

      } else {
        //Otherwise add to the aggregated Metrics
        for(String meterName: aggregatedCounters.keySet()) {
          CounterJson aggregatedCounter = aggregatedCounters.get(meterName);
          CounterJson slaveCounter = slaveCounters.get(meterName);

          if(aggregatedCounter != null && slaveCounter != null) {
            aggregatedCounter.setCount(aggregatedCounter.getCount() + slaveCounter.getCount());
          }
        }

        for(String meterName: aggregatedMeters.keySet()) {
          MeterJson aggregatedMeter = aggregatedMeters.get(meterName);
          MeterJson slaveMeter = slaveMeters.get(meterName);

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

}
