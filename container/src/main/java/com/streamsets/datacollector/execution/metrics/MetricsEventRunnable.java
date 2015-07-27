/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.datacollector.execution.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.store.PipelineStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsEventRunnable implements Runnable {
  public static final String RUNNABLE_NAME = "MetricsEventRunnable";
  private final static Logger LOG = LoggerFactory.getLogger(MetricsEventRunnable.class);
  private final Map<String, MetricRegistryJson> slaveMetrics;
  private final RuntimeInfo runtimeInfo;
  private ThreadHealthReporter threadHealthReporter;
  private final int scheduledDelay;
  private final Runner runner;
  private final EventListenerManager eventListenerManager;

  @Inject
  public MetricsEventRunnable(RuntimeInfo runtimeInfo, int scheduledDelay, Runner runner,
                              ThreadHealthReporter threadHealthReporter, EventListenerManager eventListenerManager) {
    this.runtimeInfo = runtimeInfo;
    this.scheduledDelay = scheduledDelay/1000;
    slaveMetrics = new HashMap<>();
    this.runner = runner;
    this.threadHealthReporter = threadHealthReporter;
    this.eventListenerManager = eventListenerManager;
  }



  public void clearSlaveMetrics() {
    slaveMetrics.clear();
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
      if (eventListenerManager.getMetricsEventListenerList().size() > 0 && runner.getState().getStatus().isActive()) {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String metricsJSONStr;
        if(runtimeInfo.getExecutionMode().equals(RuntimeInfo.ExecutionMode.CLUSTER)) {
          MetricRegistryJson metricRegistryJson = getAggregatedMetrics();
          metricsJSONStr = objectMapper.writer().writeValueAsString(metricRegistryJson);
        } else {
          metricsJSONStr =
            objectMapper.writer().writeValueAsString(runner.getMetrics());
        }
        eventListenerManager.broadcastMetrics(metricsJSONStr);
      }
    } catch (IOException ex) {
      LOG.warn("Error while serializing metrics, {}", ex.getMessage(), ex);
    } catch (PipelineStoreException ex) {
      LOG.warn("Error while fetching status of pipeline,  {}", ex.getMessage(), ex);
    }
  }

  public MetricRegistryJson getAggregatedMetrics() {
    MetricRegistryJson aggregatedMetrics = new MetricRegistryJson();
    Map<String, CounterJson> aggregatedCounters = null;
    Map<String, MeterJson> aggregatedMeters = null;
    List<String> slaves = new ArrayList<>();

    //FIXME<Hari>: Eventually there wont be PipelineManager. Sort this out.
    for(CallbackInfo callbackInfo : runner.getSlaveCallbackList()) {
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

    aggregatedMetrics.setCounters(aggregatedCounters);
    aggregatedMetrics.setMeters(aggregatedMeters);
    aggregatedMetrics.setSlaves(slaves);

    return aggregatedMetrics;
  }

  public int getScheduledDelay() {
    return scheduledDelay;
  }
}
