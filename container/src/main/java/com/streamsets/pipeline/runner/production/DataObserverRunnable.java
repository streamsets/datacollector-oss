/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.alerts.AlertManager;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataObserverRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DataObserverRunnable.class);
  public static final String RUNNABLE_NAME = "DataObserverRunnable";
  private static final int SCHEDULED_DELAY = -1;

  private final BlockingQueue<Object> requestQueue;
  private final DataObserverRunner dataObserverRunner;
  private final ThreadHealthReporter threadHealthReporter;

  @Inject
  public DataObserverRunnable(@Named("name")String name, @Named("rev")String rev,
                              ThreadHealthReporter threadHealthReporter, MetricRegistry metrics,
                              BlockingQueue<Object> requestQueue, AlertManager alertManager,
                              Configuration configuration) {
    this.requestQueue = requestQueue;
    this.dataObserverRunner = new DataObserverRunner(name, rev, metrics, alertManager, configuration);
    this.threadHealthReporter = threadHealthReporter;
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
          LOG.debug("Stopping the Pipeline Observer, Reason: {}", e.getMessage(), e);
          return;
        }
      }
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }

  public List<Record> getSampledRecords(String ruleId, int size) {
    return this.dataObserverRunner.getSampledRecords(ruleId, size);
  }

}
