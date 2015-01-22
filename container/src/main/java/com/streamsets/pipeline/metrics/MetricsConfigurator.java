/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsConfigurator {

  private static final String METER_SUFFIX = ".meter";
  private static final String COUNTER_SUFFIX = ".counter";
  private static final String HISTOGRAM_M5_SUFFIX = ".histogramM5";
  private static final String TIMER_SUFFIX = ".timer";
  private static final String GAUGE_SUFFIX = ".gauge";

  public static Timer createTimer(MetricRegistry metrics, String name) {
    return metrics.register(name + TIMER_SUFFIX, new Timer(new SlidingTimeWindowReservoir(60, TimeUnit.SECONDS)));
  }

  public static Meter createMeter(MetricRegistry metrics, String name) {
    return metrics.register(name + METER_SUFFIX, new ExtendedMeter());
  }

  public static Counter createCounter(MetricRegistry metrics, String name) {
    return metrics.register(name + COUNTER_SUFFIX, new Counter());
  }

  public static Histogram createHistogram5Min(MetricRegistry metrics, String name) {
    return metrics.register(name + HISTOGRAM_M5_SUFFIX, new Histogram(new ExponentiallyDecayingReservoir()));
  }

  public static Gauge<Map<String, Object>> createGuage(MetricRegistry metrics, String name,
                                                       Gauge<Map<String, Object>> guage) {
    return metrics.register(name + GAUGE_SUFFIX, guage);
  }

  public static Counter getCounter(MetricRegistry metrics, String name) {
    return metrics.getCounters().get(name + COUNTER_SUFFIX);
  }

  public static Meter getMeter(MetricRegistry metrics, String name) {
    return metrics.getMeters().get(name + METER_SUFFIX);
  }

  public static Histogram getHistogram(MetricRegistry metrics, String name) {
    return metrics.getHistograms().get(name + HISTOGRAM_M5_SUFFIX);
  }

  public static Timer getTimer(MetricRegistry metrics, String name) {
    return metrics.getTimers().get(name + TIMER_SUFFIX);
  }

  public static Gauge<Map<String, Object>> getGauge(MetricRegistry metrics, String name) {
    return metrics.getGauges().get(name + GAUGE_SUFFIX);
  }

}
