/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.alerts.AlertsChecker;
import com.streamsets.pipeline.alerts.MetricAlertsChecker;
import com.streamsets.pipeline.alerts.MetricsChecker;
import com.streamsets.pipeline.alerts.RecordSampler;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.MetricDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class ProductionObserverRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionObserverRunnable.class);

  private final String pipelineName;
  private final String rev;
  private final ProductionPipelineManagerTask pipelineManager;
  private final BlockingQueue<ProductionObserveRequest> requestQueue;
  private volatile Thread runningThread;
  private final MetricRegistry metrics;
  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;

  private final Map<String, EvictingQueue<Record>> sampleIdToRecordsMap;

  public ProductionObserverRunnable(String pipelineName, String rev, ProductionPipelineManagerTask pipelineManager,
                                    BlockingQueue<ProductionObserveRequest> requestQueue) {
    this.pipelineName = pipelineName;
    this.rev = rev;
    this.pipelineManager = pipelineManager;
    this.requestQueue = requestQueue;
    this.metrics = this.pipelineManager.getMetrics();
    this.sampleIdToRecordsMap = new ConcurrentHashMap<>();

    elEvaluator = new ELEvaluator();
    variables = new ELEvaluator.Variables();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
  }

  @Override
  public void run() {
    runningThread = Thread.currentThread();
    while(true) {
      try {
        ProductionObserveRequest productionObserveRequest = requestQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (productionObserveRequest != null && productionObserveRequest.getRuleDefinition() != null) {
          //meters
          if (productionObserveRequest.getRuleDefinition().getMetricDefinitions() != null) {
            for (MetricDefinition metricDefinition : productionObserveRequest.getRuleDefinition().getMetricDefinitions()) {
              MetricsChecker metricsHelper = new MetricsChecker(metricDefinition, metrics, variables, elEvaluator);
              metricsHelper.recordMetrics(productionObserveRequest.getSnapshot());
            }
          }

          //Alerts
          if (productionObserveRequest.getRuleDefinition().getAlertDefinitions() != null) {
            for (AlertDefinition alertDefinition : productionObserveRequest.getRuleDefinition().getAlertDefinitions()) {
              AlertsChecker alertsHelper = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
              alertsHelper.checkForAlerts(productionObserveRequest.getSnapshot());
            }
          }

          //metric alerts
          if (productionObserveRequest.getRuleDefinition().getMetricsAlertDefinitions() != null) {
            for (MetricsAlertDefinition metricsAlertDefinition : productionObserveRequest.getRuleDefinition().getMetricsAlertDefinitions()) {
              MetricAlertsChecker metricAlertsHelper = new MetricAlertsChecker(metricsAlertDefinition, metrics, variables, elEvaluator);
              metricAlertsHelper.checkForAlerts();
            }
          }

          //sampling
          if (productionObserveRequest.getRuleDefinition().getSamplingDefinitions() != null) {
            for (SamplingDefinition samplingDefinition : productionObserveRequest.getRuleDefinition().getSamplingDefinitions()) {
              RecordSampler recordSampler = new RecordSampler(pipelineName, rev, samplingDefinition,
                pipelineManager.getObserverStore(), variables, elEvaluator);
              recordSampler.sample(productionObserveRequest.getSnapshot(), sampleIdToRecordsMap);
            }
          }
        }
      } catch(InterruptedException e){
        LOG.error("Stopping the Pipeline Observer, Reason: {}", e.getMessage());
        runningThread = null;
        return;
      }
    }
  }

  public List<Record> getSampledRecords(String sampleDefinitionId) {
    //FIXME<Hari>: synchronize acccess to evicting queue
    if(sampleIdToRecordsMap.get(sampleDefinitionId) != null) {
      return new CopyOnWriteArrayList<>(sampleIdToRecordsMap.get(sampleDefinitionId));
    }
    return Collections.emptyList();
  }

  public void stop() {
    Thread thread = runningThread;
    if (thread != null) {
      thread.interrupt();
      LOG.debug("Pipeline stopped, interrupting the thread observing the pipeline");
    }
  }

}
