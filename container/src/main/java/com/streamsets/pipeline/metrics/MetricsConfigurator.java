/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

public class MetricsConfigurator {

  public static Timer createTimer(MetricRegistry metrics, String name) {
    return metrics.register(name + ".timer", new Timer(new SlidingTimeWindowReservoir(60, TimeUnit.SECONDS)));
  }

  public static Meter createMeter(MetricRegistry metrics, String name) {
    return metrics.register(name + ".meter", new Meter());
  }

  public static Counter createCounter(MetricRegistry metrics, String name) {
    return metrics.register(name + ".counter", new Counter());
  }

}
