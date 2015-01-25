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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertsChecker {

  private static final Logger LOG = LoggerFactory.getLogger(AlertsChecker.class);

  private final AlertDefinition alertDefinition;
  private final Counter matchingRecordCounter;
  private final MetricRegistry metrics;
  private final ELEvaluator.Variables variables;
  private final ELEvaluator elEvaluator;
  private final Map<String, Object> alertResponse;

  public AlertsChecker(AlertDefinition alertDefinition, MetricRegistry metrics,
                       ELEvaluator.Variables variables, ELEvaluator elEvaluator) {
    this.alertDefinition = alertDefinition;
    this.metrics = metrics;
    this.variables = variables;
    this.elEvaluator = elEvaluator;
    this.matchingRecordCounter = initMatchingRecordCounter();
    alertResponse = new HashMap<>();
  }

  private Counter initMatchingRecordCounter() {
    Counter tempCounter = MetricsConfigurator.getCounter(metrics, alertDefinition.getId());
    if(tempCounter == null) {
      tempCounter = MetricsConfigurator.createCounter(metrics, alertDefinition.getId());
    }
    return tempCounter;
  }

  public void checkForAlerts(Map<String, List<Record>> snapshot) {
    if(alertDefinition.isEnabled()) {
      String lane = alertDefinition.getLane();
      String predicate = alertDefinition.getPredicate();
      Counter recordCounter = MetricsConfigurator.getCounter(metrics, LaneResolver.getPostFixedLaneForObserver(lane));
      if(recordCounter == null) {
        recordCounter = MetricsConfigurator.createCounter(metrics, LaneResolver.getPostFixedLaneForObserver(lane));
      }

      List<Record> records = snapshot.get(LaneResolver.getPostFixedLaneForObserver(lane));
      if(records != null && !records.isEmpty()) {
        for (Record record : records) {
          recordCounter.inc();
          boolean success = false;
          try {
            success = AlertsUtil.evaluateRecord(record, predicate, variables, elEvaluator);
          } catch (ObserverException e) {
            //A faulty condition should not take down rest of the alerts with it.
            //Log and it and continue for now
            LOG.error("Error processing alert definition '{}', reason: {}", alertDefinition.getId(), e.getMessage());
            //FIXME<Hari>: remove the alert definition from store?
          }
          if (success) {
            matchingRecordCounter.inc();
          }
        }
        long threshold = Long.valueOf(alertDefinition.getThresholdValue());
        switch (alertDefinition.getThresholdType()) {
          case COUNT:
            if (matchingRecordCounter.getCount() > threshold) {
              raiseAlert(matchingRecordCounter.getCount());
            }
            break;
          case PERCENTAGE:
            if ((matchingRecordCounter.getCount() * 100 / recordCounter.getCount()) > threshold
              && recordCounter.getCount() >= alertDefinition.getMinVolume()) {
              raiseAlert(matchingRecordCounter.getCount());
            }
            break;
        }
      }
    }
  }

  private void raiseAlert(Object value) {
    alertResponse.put("currentValue", value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    if (gauge == null) {
      alertResponse.put("timestamp", System.currentTimeMillis());
    } else {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
      alertResponse.put("timestamp", ((Map<String, Object>)gauge.getValue()).get("timestamp"));
    }
    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };
    MetricsConfigurator.createGuage(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()),
      alertResponseGauge);
  }
}
