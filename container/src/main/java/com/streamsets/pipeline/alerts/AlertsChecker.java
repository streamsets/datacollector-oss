/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.ObserverException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertsChecker {

  private final AlertDefinition alertDefinition;
  private final Counter matchingRecordCounter;
  private final MetricRegistry metrics;
  private final ELEvaluator.Variables variables;
  private final ELEvaluator elEvaluator;

  public AlertsChecker(AlertDefinition alertDefinition, MetricRegistry metrics,
                       ELEvaluator.Variables variables, ELEvaluator elEvaluator) {
    this.alertDefinition = alertDefinition;
    this.metrics = metrics;
    this.variables = variables;
    this.elEvaluator = elEvaluator;
    this.matchingRecordCounter = initMatchingRecordCounter();
  }

  private Counter initMatchingRecordCounter() {
    Counter tempCounter = MetricsConfigurator.getCounter(metrics, alertDefinition.getId());
    if(tempCounter == null) {
      tempCounter = MetricsConfigurator.createCounter(metrics, alertDefinition.getId());
    }
    return tempCounter;
  }

  public void checkForAlerts(Map<String, List<Record>> snapshot) throws ObserverException {
    if(alertDefinition.isEnabled()) {
      String lane = alertDefinition.getLane();
      String predicate = alertDefinition.getPredicate();
      Counter recordCounter = MetricsConfigurator.getCounter(metrics, LaneResolver.getPostFixedLaneForObserver(lane));
      if(recordCounter == null) {
        recordCounter = MetricsConfigurator.createCounter(metrics, LaneResolver.getPostFixedLaneForObserver(lane));
      }

      List<Record> records = snapshot.get(LaneResolver.getPostFixedLaneForObserver(lane));
      for (Record record : records) {
        recordCounter.inc();
        if (AlertsUtil.evaluateRecord(record, predicate, variables, elEvaluator)) {
          matchingRecordCounter.inc();
        }
      }
      long threshold = Long.valueOf(alertDefinition.getThresholdValue());
      switch (alertDefinition.getThresholdType()) {
        case COUNT:
          if(matchingRecordCounter.getCount() > threshold) {
            raiseAlert();
          }
          break;
        case PERCENTAGE:
          if((matchingRecordCounter.getCount()/recordCounter.getCount())*100 > threshold
            && matchingRecordCounter.getCount() >= alertDefinition.getMinVolume()) {
            raiseAlert();
          }
          break;
      }
    }
  }

  private void raiseAlert() {
    if(metrics.getGauges().get(alertDefinition.getId() + ".gauge") == null) {
      Gauge<Map<String, Object>> alertResponseGauge = new Gauge<Map<String, Object>>() {
        @Override
        public Map<String, Object> getValue() {
          Map<String, Object> response = new HashMap<>();
          response.put(alertDefinition.getId(), matchingRecordCounter.getCount());
          return response;
        }
      };
      metrics.register(alertDefinition.getId() + ".gauge", alertResponseGauge);
    }
  }
}
