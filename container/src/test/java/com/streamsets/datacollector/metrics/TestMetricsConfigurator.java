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
package com.streamsets.datacollector.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.datacollector.metrics.MetricsConfigurator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestMetricsConfigurator {

  @Test
  public void testCreateTimer() {
    MetricRegistry metrics = new MetricRegistry();
    Timer timer = MetricsConfigurator.createTimer(metrics, "a", "name", "0");
    Assert.assertNotNull(timer);
    Assert.assertEquals(1, metrics.getTimers().size());
    Map.Entry<String, Timer> entry = metrics.getTimers().entrySet().iterator().next();
    Assert.assertEquals(timer, entry.getValue());
    Assert.assertEquals("a.timer", entry.getKey());
  }

  @Test
  public void testCreateMeter() {
    MetricRegistry metrics = new MetricRegistry();
    Meter meter = MetricsConfigurator.createMeter(metrics, "a", "name", "0");
    Assert.assertNotNull(meter);
    Assert.assertEquals(1, metrics.getMeters().size());
    Map.Entry<String, Meter> entry = metrics.getMeters().entrySet().iterator().next();
    Assert.assertEquals(meter, entry.getValue());
    Assert.assertEquals("a.meter", entry.getKey());
  }

  @Test
  public void testCreateCounter() {
    MetricRegistry metrics = new MetricRegistry();
    Counter counter = MetricsConfigurator.createCounter(metrics, "a", "name", "0");
    Assert.assertNotNull(counter);
    Assert.assertEquals(1, metrics.getCounters().size());
    Map.Entry<String, Counter> entry = metrics.getCounters().entrySet().iterator().next();
    Assert.assertEquals(counter, entry.getValue());
    Assert.assertEquals("a.counter", entry.getKey());
  }

  @Test
  public void testCreateHistogram() {
    MetricRegistry metrics = new MetricRegistry();
    Histogram histogram = MetricsConfigurator.createHistogram5Min(metrics, "a", "name", "0");
    Assert.assertNotNull(histogram);
    Assert.assertEquals(1, metrics.getHistograms().size());
    Map.Entry<String, Histogram> entry = metrics.getHistograms().entrySet().iterator().next();
    Assert.assertEquals(histogram, entry.getValue());
    Assert.assertEquals("a.histogramM5", entry.getKey());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTimerTwice() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createTimer(metrics, "a", "name", "0");
    MetricsConfigurator.createTimer(metrics, "a", "name", "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateMeterTwice() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createMeter(metrics, "a", "name", "0");
    MetricsConfigurator.createMeter(metrics, "a", "name", "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSCounterTwice() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createCounter(metrics, "a", "name", "0");
    MetricsConfigurator.createCounter(metrics, "a", "name", "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateHistogramTwice() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createHistogram5Min(metrics, "a", "name", "0");
    MetricsConfigurator.createHistogram5Min(metrics, "a", "name", "0");
  }

  @Test
  public void testCreateStageTimer() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createStageTimer(metrics, "a", "name", "0");
    MetricsConfigurator.createStageTimer(metrics, "a", "name", "0");

    // There should be only one metric object
    Assert.assertEquals(1, metrics.getTimers().size());
    Map.Entry<String, Timer> entry = metrics.getTimers().entrySet().iterator().next();
    Assert.assertEquals("a.timer", entry.getKey());
  }

  @Test
  public void testCreateStageMeter() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createStageMeter(metrics, "a", "name", "0");
    MetricsConfigurator.createStageMeter(metrics, "a", "name", "0");

    // There should be only one metric object
    Assert.assertEquals(1, metrics.getMeters().size());
    Map.Entry<String, Meter> entry = metrics.getMeters().entrySet().iterator().next();
    Assert.assertEquals("a.meter", entry.getKey());
  }

  @Test
  public void testCreateStageCounter() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createStageCounter(metrics, "a", "name", "0");
    MetricsConfigurator.createStageCounter(metrics, "a", "name", "0");

    // There should be only one metric object
    Assert.assertEquals(1, metrics.getCounters().size());
    Map.Entry<String, Counter> entry = metrics.getCounters().entrySet().iterator().next();
    Assert.assertEquals("a.counter", entry.getKey());
  }

  @Test
  public void testCreateStageHistogram() {
    MetricRegistry metrics = new MetricRegistry();
    MetricsConfigurator.createStageHistogram5Min(metrics, "a", "name", "0");
    MetricsConfigurator.createStageHistogram5Min(metrics, "a", "name", "0");

    Assert.assertEquals(1, metrics.getHistograms().size());
    Map.Entry<String, Histogram> entry = metrics.getHistograms().entrySet().iterator().next();
    Assert.assertEquals("a.histogramM5", entry.getKey());
  }

}
