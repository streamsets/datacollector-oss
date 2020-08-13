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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.production.DataRulesEvaluationRequest;
import com.streamsets.datacollector.runner.production.PipelineErrorNotificationRequest;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataObserverRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DataObserverRunnable.class);
  public static final String RUNNABLE_NAME = "DataObserverRunnable";
  private static final int SCHEDULED_DELAY = -1;

  private BlockingQueue<Object> requestQueue;
  private final DataObserverRunner dataObserverRunner;
  private final ThreadHealthReporter threadHealthReporter;

  public DataObserverRunnable(
      String name,
      String rev,
      ThreadHealthReporter threadHealthReporter,
      MetricRegistry metrics,
      AlertManager alertManager,
      Configuration configuration,
      RuntimeInfo runtimeInfo,
      Map<String, Object> resolvedParameters
  ) {
    this.dataObserverRunner = new DataObserverRunner(
        name,
        rev,
        metrics,
        alertManager,
        configuration,
        resolvedParameters
    );
    this.threadHealthReporter = threadHealthReporter;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  public void setRequestQueue(BlockingQueue<Object> requestQueue) {
    this.requestQueue = requestQueue;
  }

  public void setStatsQueue(BlockingQueue<Record> statsQueue) {
    this.dataObserverRunner.setStatsQueue(statsQueue);
  }

  @Override
  public void run() {
    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(originalName + "-" + RUNNABLE_NAME);
    try {
      while (true) {
        threadHealthReporter.reportHealth(RUNNABLE_NAME, SCHEDULED_DELAY, System.currentTimeMillis());
        try {
          Object request = requestQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (request != null) {
            if (request instanceof DataRulesEvaluationRequest) {
              //data monitoring
              dataObserverRunner.handleDataRulesEvaluationRequest((DataRulesEvaluationRequest) request);
            } else if (request instanceof RulesConfigurationChangeRequest) {
              //configuration changes
              dataObserverRunner.handleConfigurationChangeRequest((RulesConfigurationChangeRequest) request);
            } else if (request instanceof PipelineErrorNotificationRequest) {
              dataObserverRunner.handlePipelineErrorNotificationRequest((PipelineErrorNotificationRequest) request);
            } else {
              LOG.error("Unknown request: " + request.getClass().getName());
            }
          }
        } catch (InterruptedException e) {
          LOG.debug("Stopping the Pipeline Observer, Reason: {}", e.toString(), e);
          return;
        }
      }
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }

  public void setMetricRegistryJson(MetricRegistryJson metricRegistryJson) {
    dataObserverRunner.setMetricRegistryJson(metricRegistryJson);
  }

  public List<SampledRecord> getSampledRecords(String ruleId, int size) {
    return this.dataObserverRunner.getSampledRecords(ruleId, size);
  }

}
