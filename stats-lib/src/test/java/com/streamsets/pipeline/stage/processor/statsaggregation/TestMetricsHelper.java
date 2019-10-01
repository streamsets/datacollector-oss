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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestMetricsHelper {

  private static MetricRegistry metricRegistry = new MetricRegistry();

  @Test
  public void testCreateAndInitTimer() {
    MetricRegistryJson metricRegistryJson = new MetricRegistryJson();
    Map<String, TimerJson> timer = new HashMap<>();
    TimerJson timerJson = new TimerJson();
    timerJson.setCount(100);
    timerJson.setMean(10);
    timer.put("t" + MetricsConfigurator.TIMER_SUFFIX, timerJson);
    metricRegistryJson.setTimers(timer);

    Timer t = MetricsHelper.createAndInitTimer(metricRegistryJson, metricRegistry, "y", "x", "1");
    Assert.assertEquals(0, t.getCount());

    t = MetricsHelper.createAndInitTimer(metricRegistryJson, metricRegistry, "t", "x", "1");
    Assert.assertEquals(1, t.getCount());
    // convert milli seconds to nano seconds
    Assert.assertEquals(100 * 1000 *1000, t.getSnapshot().getValues()[0]);
  }

  @Test
  public void testCreateAndInitCounter() {
    MetricRegistryJson metricRegistryJson = new MetricRegistryJson();
    Map<String, CounterJson> counter = new HashMap<>();
    CounterJson counterJson = new CounterJson();
    counterJson.setCount(100);
    counter.put("t" + MetricsConfigurator.COUNTER_SUFFIX, counterJson);
    metricRegistryJson.setCounters(counter);

    Counter c = MetricsHelper.createAndInitCounter(metricRegistryJson, metricRegistry, "y", "x", "1");
    Assert.assertEquals(0, c.getCount());

    c = MetricsHelper.createAndInitCounter(metricRegistryJson, metricRegistry, "t", "x", "1");
    Assert.assertEquals(100, c.getCount());
  }

  @Test
  public void testCreateAndInitHistogram() {
    MetricRegistryJson metricRegistryJson = new MetricRegistryJson();
    Map<String, HistogramJson> histogram = new HashMap<>();
    HistogramJson histogramJson = new HistogramJson();
    histogramJson.setCount(100);
    histogram.put("t" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, histogramJson);
    metricRegistryJson.setHistograms(histogram);

    Histogram h = MetricsHelper.createAndInitHistogram(metricRegistryJson, metricRegistry, "y", "x", "1");
    Assert.assertEquals(0, h.getCount());

    h = MetricsHelper.createAndInitHistogram(metricRegistryJson, metricRegistry, "t", "x", "1");
    Assert.assertEquals(1, h.getCount());
    Assert.assertEquals(100,h.getSnapshot().getValues()[0]);

  }

  @Test
  public void testCreateAndInitMeter() {
    MetricRegistryJson metricRegistryJson = new MetricRegistryJson();
    Map<String, MeterJson> meterJsonMap = new HashMap<>();
    MeterJson meterJson = new MeterJson();
    meterJson.setCount(100);
    meterJsonMap.put("t" + MetricsConfigurator.METER_SUFFIX, meterJson);
    metricRegistryJson.setMeters(meterJsonMap);

    Meter m = MetricsHelper.createAndInitMeter(metricRegistryJson, metricRegistry, "y", "x", "1");
    Assert.assertEquals(0, m.getCount());

    m = MetricsHelper.createAndInitMeter(metricRegistryJson, metricRegistry, "t", "x", "1");
    Assert.assertEquals(100, m.getCount());
  }
}
