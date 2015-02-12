/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.metrics.ExtendedMeter;
import com.streamsets.pipeline.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MetricRuleEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(MetricRuleEvaluator.class);
  private static final String VAL = "value()";

  private final MetricsRuleDefinition metricsRuleDefinition;
  private final MetricRegistry metrics;
  private final ELEvaluator.Variables variables;
  private final ELEvaluator elEvaluator;
  private final List<String> emailIds;
  private final AlertManager alertManager;

  public MetricRuleEvaluator(MetricsRuleDefinition metricsRuleDefinition, MetricRegistry metricRegistry,
                             ELEvaluator.Variables variables, ELEvaluator elEvaluator, AlertManager alertManager,
                             List<String> emailIds) {
    this.metricsRuleDefinition = metricsRuleDefinition;
    this.metrics = metricRegistry;
    this.variables = variables;
    this.elEvaluator = elEvaluator;
    this.emailIds = emailIds;
    this.alertManager = alertManager;
  }

  public void checkForAlerts() {
    if(metricsRuleDefinition.isEnabled()) {
      switch(metricsRuleDefinition.getMetricType()) {
        case HISTOGRAM:
          checkForHistogramAlerts();
          break;
        case METER:
          checkForMeterAlerts();
          break;
        case COUNTER:
          checkForCounterAlerts();
          break;
        case TIMER:
          checkForTimerAlerts();
          break;
      }
    }
  }

  private void checkForTimerAlerts() {
    Timer t = metrics.getTimers().get(metricsRuleDefinition.getMetricId());
    if(t != null) {
      Object value = null;
      switch (metricsRuleDefinition.getMetricElement()) {
        case TIMER_COUNT:
          value = t.getCount();
          break;
        case TIMER_M15_RATE:
          value = t.getFifteenMinuteRate();
          break;
        case TIMER_M1_RATE:
          value = t.getOneMinuteRate();
          break;
        case TIMER_M5_RATE:
          value = t.getFiveMinuteRate();
          break;
        case TIMER_MAX:
          value = t.getSnapshot().getMax();
          break;
        case TIMER_MEAN:
          value = t.getSnapshot().getMean();
          break;
        case TIMER_MEAN_RATE:
          value = t.getMeanRate();
          break;
        case TIMER_MIN:
          value = t.getSnapshot().getMin();
          break;
        case TIMER_P50:
          value = t.getSnapshot().getMedian();
          break;
        case TIMER_P75:
          value = t.getSnapshot().get75thPercentile();
          break;
        case TIMER_P95:
          value = t.getSnapshot().get95thPercentile();
          break;
        case TIMER_P98:
          value = t.getSnapshot().get98thPercentile();
          break;
        case TIMER_P99:
          value = t.getSnapshot().get99thPercentile();
          break;
        case TIMER_P999:
          value = t.getSnapshot().get999thPercentile();
          break;
        case TIMER_STD_DEV:
          value = t.getSnapshot().getStdDev();
          break;
      }
      evaluate(value);
    }
  }

  private void checkForCounterAlerts() {
    Counter c = metrics.getCounters().get(metricsRuleDefinition.getMetricId());
    if(c !=null) {
      Object value = null;
      switch (metricsRuleDefinition.getMetricElement()) {
        case COUNTER_COUNT:
          value = c.getCount();
          break;
      }
      evaluate(value);
    }
  }

  private void checkForMeterAlerts() {
    ExtendedMeter m = (ExtendedMeter) metrics.getMeters().get(metricsRuleDefinition.getMetricId());
    if(m != null) {
      Object value = null;
      switch (metricsRuleDefinition.getMetricElement()) {
        case METER_COUNT:
          value = m.getCount();
          break;
        case METER_H12_RATE:
          value = m.getTwelveHourRate();
          break;
        case METER_H1_RATE:
          value = m.getOneHourRate();
          break;
        case METER_H24_RATE:
          value = m.getTwentyFourHourRate();
          break;
        case METER_H6_RATE:
          value = m.getSixHourRate();
          break;
        case METER_M15_RATE:
          value = m.getFifteenMinuteRate();
          break;
        case METER_M1_RATE:
          value = m.getOneMinuteRate();
          break;
        case METER_M30_RATE:
          value = m.getThirtyMinuteRate();
          break;
        case METER_M5_RATE:
          value = m.getFiveMinuteRate();
          break;
        case METER_MEAN_RATE:
          value = m.getMeanRate();
          break;
      }
      evaluate(value);
    }
  }

  private void checkForHistogramAlerts() {
    Histogram h = metrics.getHistograms().get(metricsRuleDefinition.getMetricId());
    if (h != null) {
      Object value = null;
      switch (metricsRuleDefinition.getMetricElement()) {
        case HISTOGRAM_COUNT:
          value = h.getCount();
          break;
        case HISTOGRAM_MAX:
          value = h.getSnapshot().getMax();
          break;
        case HISTOGRAM_MEAN:
          value = h.getSnapshot().getMean();
          break;
        case HISTOGRAM_MIN:
          value = h.getSnapshot().getMin();
          break;
        case HISTOGRAM_MEDIAN:
          value = h.getSnapshot().getMedian();
          break;
        case HISTOGRAM_P75:
          value = h.getSnapshot().get75thPercentile();
          break;
        case HISTOGRAM_P95:
          value = h.getSnapshot().get95thPercentile();
          break;
        case HISTOGRAM_P98:
          value = h.getSnapshot().get98thPercentile();
          break;
        case HISTOGRAM_P99:
          value = h.getSnapshot().get99thPercentile();
          break;
        case HISTOGRAM_P999:
          value = h.getSnapshot().get999thPercentile();
          break;
        case HISTOGRAM_STD_DEV:
          value = h.getSnapshot().getStdDev();
          break;
      }
      evaluate(value);
    }
  }

  private void evaluate(Object value) {
    //predicate String is of the form "val()<200" or "val() < 200 && val() > 100" etc
    //replace val() with the actual value, append dollar and curly braces and evaluate the resulting EL expression
    // string
    String predicateWithValue = metricsRuleDefinition.getCondition().replace(VAL, String.valueOf(value));
    try {
      if (AlertsUtil.evaluateExpression(predicateWithValue, variables, elEvaluator)) {
        alertManager.alert(value, emailIds, metricsRuleDefinition);
      }
    } catch (ObserverException e) {
      //A faulty condition should not take down rest of the alerts with it.
      //Log and it and continue for now
      LOG.error("Error processing metric definition alert '{}', reason: {}", metricsRuleDefinition.getId(),
        e.getMessage(), e);
    }
  }
}
