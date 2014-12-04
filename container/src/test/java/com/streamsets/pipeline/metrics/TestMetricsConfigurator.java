/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestMetricsConfigurator {

  @Test
  public void testCreateTimer() {
    MetricRegistry metrics = new MetricRegistry();
    Timer timer = MetricsConfigurator.createTimer(metrics, "a");
    Assert.assertNotNull(timer);
    Assert.assertEquals(1, metrics.getTimers().size());
    Map.Entry<String, Timer> entry = metrics.getTimers().entrySet().iterator().next();
    Assert.assertEquals(timer, entry.getValue());
    Assert.assertEquals("a.timer", entry.getKey());
  }


  @Test
  public void testCreateMeter() {
    MetricRegistry metrics = new MetricRegistry();
    Meter meter = MetricsConfigurator.createMeter(metrics, "a");
    Assert.assertNotNull(meter);
    Assert.assertEquals(1, metrics.getMeters().size());
    Map.Entry<String, Meter> entry = metrics.getMeters().entrySet().iterator().next();
    Assert.assertEquals(meter, entry.getValue());
    Assert.assertEquals("a.meter", entry.getKey());
  }


  @Test
  public void testCreateCounter() {
    MetricRegistry metrics = new MetricRegistry();
    Counter counter = MetricsConfigurator.createCounter(metrics, "a");
    Assert.assertNotNull(counter);
    Assert.assertEquals(1, metrics.getCounters().size());
    Map.Entry<String, Counter> entry = metrics.getCounters().entrySet().iterator().next();
    Assert.assertEquals(counter, entry.getValue());
    Assert.assertEquals("a.counter", entry.getKey());
  }
}
