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
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.RuleELRegistry;
import com.streamsets.datacollector.metrics.ExtendedMeter;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.PipeRunner;
import com.streamsets.datacollector.runner.RuntimeStats;
import com.streamsets.datacollector.util.ObserverException;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;

public class MetricRuleEvaluatorHelper {

  private static final String VAL = "value()";
  private static final String TIME_NOW = "time:now()";
  private static final String START_TIME = "pipeline:startTime()";
  private static final ELEvaluator EL_EVALUATOR =  new ELEvaluator(
    "condition", false, ConcreteELDefinitionExtractor.get(),
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
      case TIME_OF_LAST_RECEIVED_RECORD:
        value =  runtimeStats.getTimeOfLastReceivedRecord();
        break;
      case LAST_BATCH_INPUT_RECORDS_COUNT:
        value =  runtimeStats.getLastBatchInputRecordsCount();
        break;
      case LAST_BATCH_OUTPUT_RECORDS_COUNT:
        value =  runtimeStats.getLastBatchOutputRecordsCount();
        break;
      case LAST_BATCH_ERROR_RECORDS_COUNT:
        value =  runtimeStats.getLastBatchErrorRecordsCount();
        break;
      case LAST_BATCH_ERROR_MESSAGES_COUNT:
        value =  runtimeStats.getLastBatchErrorMessagesCount();
        break;
      default:
        throw new IllegalStateException("Unexpected metric element type " + metricElement);
    }
    return value;
  }


  public static boolean evaluate(long pipelineStartTime, Object value, String condition) throws ObserverException {
    //predicate String is of the form "val()<200" or "val() < 200 && val() > 100" etc
    //replace val() with the actual value, append dollar and curly braces and evaluate the resulting EL expression
    // string
    String predicateWithValue = condition
      .replace(VAL, String.valueOf(value))
      .replace(TIME_NOW, System.currentTimeMillis() + "")
      .replace(START_TIME, pipelineStartTime + "");
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

  /**
   * Get metric value for given rule evaluation.
   *
   * @param metrics Metric Registry for the pipeline.
   * @param metricId Name of the metric where the value is "usually" stored. This method cover mappings of metrics
   *                 that got historically moved.
   * @param metricType Type of the metric
   * @param metricElement Value that caller needs in order to assert the right condition.
   * @return Requested metric value or null if it doesn't exists
   * @throws ObserverException
   */
  public static Object getMetricValue(
    MetricRegistry metrics,
    String metricId,
    MetricType metricType,
    MetricElement metricElement
  ) throws ObserverException {
    // We moved the logic of CURRENT_BATCH_AGE and TIME_IN_CURRENT_STAGE due to multi-threaded framework
    if(metricElement.isOneOf(MetricElement.CURRENT_BATCH_AGE, MetricElement.TIME_IN_CURRENT_STAGE)) {
      switch (metricElement) {
        case CURRENT_BATCH_AGE:
          return getTimeFromRunner(metrics, PipeRunner.METRIC_BATCH_START_TIME);
        case TIME_IN_CURRENT_STAGE:
          return getTimeFromRunner(metrics, PipeRunner.METRIC_STAGE_START_TIME);
        default:
          throw new IllegalStateException(Utils.format("Unknown metric type '{}'", metricType));
      }
    }

    // Default path
    Metric metric = getMetric(
        metrics,
        metricId,
        metricType
    );

    if(metric != null) {
      return getMetricValue(metricElement, metricType, metric);
    }

    return null;
  }

  /**
   * Return calculated metric - from all the runners that are available for given pipeline, return the biggest difference
   * between given metric and System.currentTimeMillis(). The semantic is that the runner metric stores start time of
   * certain events (batch start time, stage start time) and we need to find out what is the longer running time for one
   * of those metrics.
   *
   * @param metrics Metric registry for given pipeline.
   * @param runnerMetricName Name of the PipeRunner metric that contains start time of given action.
   * @return
   */
  private static long getTimeFromRunner(
    MetricRegistry metrics,
    String runnerMetricName
  ) {
    // First get number of total runners from the runtime gauge
    RuntimeStats runtimeStats = (RuntimeStats) ((Gauge)getMetric(metrics, "RuntimeStatsGauge.gauge", MetricType.GAUGE)).getValue();
    long totalRunners = runtimeStats.getTotalRunners();

    long currentTime = System.currentTimeMillis();
    long maxTime = 0;

    // Then iterate over all runners and find the biggest time difference
    for(int runnerId = 0; runnerId < totalRunners; runnerId++) {
      Map<String, Object> runnerMetrics = (Map<String, Object>) ((Gauge)getMetric(metrics, "runner." + runnerId, MetricType.GAUGE)).getValue();

      // Get current value
      long value = (long) runnerMetrics.getOrDefault(runnerMetricName, 0L);

      // Zero means that the runner is not in use at all and thus calculating running time makes no sense
      if(value == 0) {
        continue;
      }

      long runTime = currentTime - value;
      if(maxTime < runTime) {
        maxTime = runTime;
      }
    }

    return maxTime;
  }
}
