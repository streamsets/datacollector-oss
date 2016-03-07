/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.RuleELRegistry;
import com.streamsets.datacollector.metrics.ExtendedMeter;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.RuntimeStats;
import com.streamsets.datacollector.util.ObserverException;
import com.streamsets.pipeline.api.impl.Utils;

public class MetricRuleEvaluatorHelper {

  private static final String VAL = "value()";
  private static final String TIME_NOW = "time:now()";
  private static final ELEvaluator EL_EVALUATOR =  new ELEvaluator(
    "condition",
    RuleELRegistry.getRuleELs(RuleELRegistry.GENERAL)
  );

  public static Metric getMetric(MetricRegistry metrics, String metricId, MetricType metricType) {
    Metric metric;
    switch (metricType) {
      case HISTOGRAM:
        metric = MetricsConfigurator.getHistogram(metrics, metricId);
        break;
      case METER:
        metric = MetricsConfigurator.getMeter(metrics, metricId);
        break;
      case COUNTER:
        metric = MetricsConfigurator.getCounter(metrics, metricId);
        break;
      case TIMER:
        metric = MetricsConfigurator.getTimer(metrics, metricId);
        break;
      case GAUGE:
        metric = MetricsConfigurator.getGauge(metrics, metricId);
        break;
      default :
        throw new IllegalArgumentException(Utils.format("Unknown metric type '{}'", metricType));
    }
    return metric;
  }

  public static Object getTimerValue(Timer t, MetricElement metricElement) {
    Object value = null;
    if (t != null) {
      switch (metricElement) {
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
        default:
          throw new IllegalArgumentException("Unexpected metric element " + metricElement);
      }
    }
    return value;
  }

  public static Object getCounterValue(Counter counter, MetricElement metricElement) {
    Object value;
    switch (metricElement) {
      case COUNTER_COUNT:
        value = counter.getCount();
        break;
      default:
        throw new IllegalStateException("Unexpected metric element type " + metricElement);
    }
    return value;
  }

  public static Object getMeterValue(ExtendedMeter meter, MetricElement metricElement) {
    Object value;
    switch (metricElement) {
      case METER_COUNT:
        value = meter.getCount();
        break;
      case METER_H12_RATE:
        value = meter.getTwelveHourRate();
        break;
      case METER_H1_RATE:
        value = meter.getOneHourRate();
        break;
      case METER_H24_RATE:
        value = meter.getTwentyFourHourRate();
        break;
      case METER_H6_RATE:
        value = meter.getSixHourRate();
        break;
      case METER_M15_RATE:
        value = meter.getFifteenMinuteRate();
        break;
      case METER_M1_RATE:
        value = meter.getOneMinuteRate();
        break;
      case METER_M30_RATE:
        value = meter.getThirtyMinuteRate();
        break;
      case METER_M5_RATE:
        value = meter.getFiveMinuteRate();
        break;
      case METER_MEAN_RATE:
        value = meter.getMeanRate();
        break;
      default:
        throw new IllegalStateException("Unexpected metric element type " + metricElement);
    }
    return value;
  }

  public static Object getHistogramValue(Histogram histogram, MetricElement metricElement) {
    Object value;
    switch (metricElement) {
      case HISTOGRAM_COUNT:
        value = histogram.getCount();
        break;
      case HISTOGRAM_MAX:
        value = histogram.getSnapshot().getMax();
        break;
      case HISTOGRAM_MEAN:
        value = histogram.getSnapshot().getMean();
        break;
      case HISTOGRAM_MIN:
        value = histogram.getSnapshot().getMin();
        break;
      case HISTOGRAM_MEDIAN:
        value = histogram.getSnapshot().getMedian();
        break;
      case HISTOGRAM_P75:
        value = histogram.getSnapshot().get75thPercentile();
        break;
      case HISTOGRAM_P95:
        value = histogram.getSnapshot().get95thPercentile();
        break;
      case HISTOGRAM_P98:
        value = histogram.getSnapshot().get98thPercentile();
        break;
      case HISTOGRAM_P99:
        value = histogram.getSnapshot().get99thPercentile();
        break;
      case HISTOGRAM_P999:
        value = histogram.getSnapshot().get999thPercentile();
        break;
      case HISTOGRAM_STD_DEV:
        value = histogram.getSnapshot().getStdDev();
        break;
      default:
        throw new IllegalStateException("Unexpected metric element type " + metricElement);
    }
    return value;
  }

  public static Object getGaugeValue(Gauge gauge, MetricElement metricElement) {
    Object value;
    RuntimeStats runtimeStats = (RuntimeStats)gauge.getValue();
    switch (metricElement) {
      case CURRENT_BATCH_AGE:
        value =  runtimeStats.getCurrentBatchAge();
        break;
      case TIME_IN_CURRENT_STAGE:
        value =  runtimeStats.getTimeInCurrentStage();
        break;
      case TIME_OF_LAST_RECEIVED_RECORD:
        value =  runtimeStats.getTimeOfLastReceivedRecord();
        break;
      default:
        throw new IllegalStateException("Unexpected metric element type " + metricElement);
    }
    return value;
  }


  public static boolean evaluate(Object value, String condition) throws ObserverException {
    //predicate String is of the form "val()<200" or "val() < 200 && val() > 100" etc
    //replace val() with the actual value, append dollar and curly braces and evaluate the resulting EL expression
    // string
    String predicateWithValue = condition
      .replace(VAL, String.valueOf(value))
      .replace(TIME_NOW, System.currentTimeMillis() + "");
    return AlertsUtil.evaluateExpression(predicateWithValue, new ELVariables(), EL_EVALUATOR);
  }

  public static Object getMetricValue(
    MetricElement metricElement,
    MetricType metricType,
    Metric metric
  ) throws ObserverException {
    Object value;
    switch (metricType) {
      case HISTOGRAM:
        value = MetricRuleEvaluatorHelper.getHistogramValue((Histogram) metric, metricElement);
        break;
      case METER:
        value = MetricRuleEvaluatorHelper.getMeterValue((ExtendedMeter) metric, metricElement);
        break;
      case COUNTER:
        value = MetricRuleEvaluatorHelper.getCounterValue((Counter) metric, metricElement);
        break;
      case TIMER:
        value = MetricRuleEvaluatorHelper.getTimerValue((Timer) metric, metricElement);
        break;
      case GAUGE:
        value = MetricRuleEvaluatorHelper.getGaugeValue((Gauge) metric, metricElement);
        break;
      default :
        throw new IllegalArgumentException(Utils.format("Unknown metric type '{}'", metricType));
    }
    return value;
  }
}
