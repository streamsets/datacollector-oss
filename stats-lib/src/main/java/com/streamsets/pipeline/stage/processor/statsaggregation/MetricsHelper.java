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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.CounterJson;
import com.streamsets.datacollector.event.json.HistogramJson;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.event.json.TimerJson;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsHelper {

  static Timer createAndInitTimer(
      MetricRegistryJson metricRegistryJson,
      MetricRegistry metrics,
      String timerName,
      String pipelineName,
      String revision
  ) {
    Timer timer = MetricsConfigurator.createTimer(metrics, timerName, pipelineName, revision);
    if (metricRegistryJson != null) {
      Map<String, TimerJson> timers = metricRegistryJson.getTimers();
      if (null != timers && timers.containsKey(timerName + MetricsConfigurator.TIMER_SUFFIX)) {
        TimerJson timerJson = timers.get(timerName + MetricsConfigurator.TIMER_SUFFIX);
        if (timerJson != null) {
          timer.update(timerJson.getCount(), TimeUnit.MILLISECONDS);
        }
      }
    }
    return timer;
  }

  static Histogram createAndInitHistogram(
      MetricRegistryJson metricRegistryJson,
      MetricRegistry metrics,
      String histogramName,
      String pipelineName,
      String revision
  ) {
    Histogram histogram5Min = MetricsConfigurator.createHistogram5Min(metrics, histogramName, pipelineName, revision);
    if (metricRegistryJson != null) {
      Map<String, HistogramJson> histograms = metricRegistryJson.getHistograms();
      if (null != histograms && histograms.containsKey(histogramName + MetricsConfigurator.HISTOGRAM_M5_SUFFIX)) {
        HistogramJson histogramJson = histograms.get(histogramName + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
        if (histogramJson != null) {
          histogram5Min.update(histogramJson.getCount());
        }
      }
    }
    return histogram5Min;
  }

  static Meter createAndInitMeter(
      MetricRegistryJson metricRegistryJson,
      MetricRegistry metrics,
      String meterName,
      String pipelineName,
      String revision
  ) {
    Meter meter = MetricsConfigurator.createMeter(metrics, meterName, pipelineName, revision);
    if (metricRegistryJson != null) {
      Map<String, MeterJson> meters = metricRegistryJson.getMeters();
      if (null != meters && meters.containsKey(meterName + MetricsConfigurator.METER_SUFFIX)) {
        MeterJson meterJson = meters.get(meterName + MetricsConfigurator.METER_SUFFIX);
        if (meterJson != null) {
          meter.mark(meterJson.getCount());
        }
      }
    }
    return meter;
  }

  static Counter createAndInitCounter(
      MetricRegistryJson metricRegistryJson,
      MetricRegistry metrics,
      String counterName,
      String pipelineName,
      String revision
  ) {
    Counter counter = MetricsConfigurator.createCounter(metrics, counterName, pipelineName, revision);
    if (metricRegistryJson != null) {
      Map<String, CounterJson> counters = metricRegistryJson.getCounters();
      if (null != counters && counters.containsKey(counterName + MetricsConfigurator.COUNTER_SUFFIX)) {
        CounterJson counterJson = counters.get(counterName + MetricsConfigurator.COUNTER_SUFFIX);
        if (counterJson != null) {
          counter.inc(counterJson.getCount());
        }
      }
    }
    return counter;
  }
}
