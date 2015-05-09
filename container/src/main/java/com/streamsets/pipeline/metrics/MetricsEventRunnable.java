/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.metrics;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.restapi.bean.CounterJson;
import com.streamsets.pipeline.restapi.bean.MeterJson;
import com.streamsets.pipeline.restapi.bean.MetricRegistryJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsEventRunnable implements Runnable {
  private final static Logger LOG = LoggerFactory.getLogger(MetricsEventRunnable.class);
  private List<MetricsEventListener> metricsEventListenerList = new ArrayList<>();
  private Map<String, MetricRegistryJson> slaveMetrics;

  private final PipelineManager pipelineManager;
  private final RuntimeInfo runtimeInfo;

  public MetricsEventRunnable(PipelineManager pipelineManager, RuntimeInfo runtimeInfo) {
    this.pipelineManager = pipelineManager;
    this.runtimeInfo = runtimeInfo;
    slaveMetrics = new HashMap<>();
  }

  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventListenerList.add(metricsEventListener);
  }

  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventListenerList.remove(metricsEventListener);
  }

  public void clearSlaveMetrics() {
    slaveMetrics.clear();
  }

  public void run() {
    try {
      if (metricsEventListenerList.size() > 0 && pipelineManager.getPipelineState() != null &&
        pipelineManager.getPipelineState().getState() == State.RUNNING) {
        ObjectMapper objectMapper = ObjectMapperFactory.get();

        String metricsJSONStr;

        if(runtimeInfo.getExecutionMode().equals(RuntimeInfo.ExecutionMode.CLUSTER)) {
          MetricRegistryJson metricRegistryJson = getAggregatedMetrics();
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistryJson);
        } else {
          metricsJSONStr = objectMapper.writer().writeValueAsString(pipelineManager.getMetrics());
        }

        for(MetricsEventListener alertEventListener : metricsEventListenerList) {
          try {
            alertEventListener.notification(metricsJSONStr);
          } catch(Exception ex) {
            LOG.warn("Error while notifying metrics, {}", ex.getMessage(), ex);
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Error while serializing metrics, {}", ex.getMessage(), ex);
    }
  }

  private MetricRegistryJson getAggregatedMetrics() {
    MetricRegistryJson aggregatedMetrics = new MetricRegistryJson();
    Map<String, CounterJson> aggregatedCounters = null;
    Map<String, MeterJson> aggregatedMeters = null;
    List<String> slaves = new ArrayList<>();

    for(CallbackInfo callbackInfo : pipelineManager.getSlaveCallbackList()) {
      slaves.add(callbackInfo.getSdcURL());
      MetricRegistryJson metricRegistryJson = callbackInfo.getMetricRegistryJson();
      if(metricRegistryJson != null) {
        slaveMetrics.put(callbackInfo.getSdcSlaveToken(), callbackInfo.getMetricRegistryJson());
      }
    }

    for(String slaveSdcToken: slaveMetrics.keySet()) {
      MetricRegistryJson metrics = slaveMetrics.get(slaveSdcToken);
      if(aggregatedCounters == null) {
        //First Slave Metrics
        aggregatedCounters = metrics.getCounters();
        aggregatedMeters = metrics.getMeters();
      } else {
        //Otherwise add to the aggregated Metrics
        Map<String, CounterJson> slaveCounters = metrics.getCounters();
        for(String meterName: aggregatedCounters.keySet()) {
          CounterJson aggregatedCounter = aggregatedCounters.get(meterName);
          CounterJson slaveCounter = slaveCounters.get(meterName);
          aggregatedCounter.setCount(aggregatedCounter.getCount() + slaveCounter.getCount());
        }

        Map<String, MeterJson> slaveMeters = metrics.getMeters();
        for(String meterName: aggregatedMeters.keySet()) {
          MeterJson aggregatedMeter = aggregatedMeters.get(meterName);
          MeterJson slaveMeter = slaveMeters.get(meterName);
          aggregatedMeter.setCount(aggregatedMeter.getCount() + slaveMeter.getCount());
        }
      }
    }

    aggregatedMetrics.setCounters(aggregatedCounters);
    aggregatedMetrics.setMeters(aggregatedMeters);
    aggregatedMetrics.setSlaves(slaves);

    return aggregatedMetrics;
  }
}
