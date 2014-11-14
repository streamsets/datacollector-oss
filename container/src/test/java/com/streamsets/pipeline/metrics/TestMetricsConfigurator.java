/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
