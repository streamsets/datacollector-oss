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
package com.streamsets.datacollector.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetricsConfigurator {
  public static final String JMX_PREFIX = "sdc.pipeline.";

  public static final String METER_SUFFIX = ".meter";
  public static final String COUNTER_SUFFIX = ".counter";
  public static final String HISTOGRAM_M5_SUFFIX = ".histogramM5";
  public static final String TIMER_SUFFIX = ".timer";
  public static final String GAUGE_SUFFIX = ".gauge";

  private static MetricRegistry sdcMetrics;
  private static List<String> runningPipelines = new ArrayList<>();

  private static String metricName(String name, String type) {
    if (name.endsWith(type)) {
      return name;
    }
    return name + type;
  }

  private static String jmxNamePrefix(String pipelineName, String pipelineRev) {
    return JMX_PREFIX + pipelineName + "." + pipelineRev + ".";
  }

  //we need to use the AccessController.doPrivilege for sdcMetric calls because there are calls to JMX MBs
  //and user-libs stages fail otherwise due to lesser privileges
  public static Timer createTimer(MetricRegistry metrics, String name, final String pipelineName,
                                  final String pipelineRev) {
    final String timerName = metricName(name, TIMER_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final Timer timer = new Timer(new SlidingTimeWindowReservoir(60, TimeUnit.SECONDS));
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null && runningPipelines.contains(jmxNamePrefix)) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          String metricName = jmxNamePrefix + timerName;
          if(!metricRegistry.getNames().contains(metricName)) {
            metricRegistry.register(metricName, timer);
          }
          return null;
        }
      });
    }
    return metrics.register(timerName, timer);
  }

  public static Meter createMeter(MetricRegistry metrics, String name, final String pipelineName,
                                  final String pipelineRev) {
    final String meterName = metricName(name, METER_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final ExtendedMeter meter = new ExtendedMeter();
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null && runningPipelines.contains(jmxNamePrefix)) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          String metricName = jmxNamePrefix  + meterName;
          if(!metricRegistry.getNames().contains(metricName)) {
            metricRegistry.register(metricName, meter);
          }

          return null;
        }
      });
    }
    return metrics.register(meterName, meter);
  }

  public static Counter createCounter(MetricRegistry metrics, String name, final String pipelineName,
                                      final String pipelineRev) {
    final String counterName = metricName(name, COUNTER_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final Counter counter = new Counter();
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null && runningPipelines.contains(jmxNamePrefix)) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          String metricName = jmxNamePrefix  + counterName;
          if(!metricRegistry.getNames().contains(metricName)) {
            metricRegistry.register(metricName, counter);
          }
          return null;
        }
      });
    }
    return metrics.register(counterName, counter);
  }

  public static Histogram createHistogram5Min(MetricRegistry metrics, String name, final String pipelineName,
                                              final String pipelineRev) {
    final String histogramName = metricName(name, HISTOGRAM_M5_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null && runningPipelines.contains(jmxNamePrefix)) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          String metricName = jmxNamePrefix  + histogramName;
          if(!metricRegistry.getNames().contains(metricName)) {
            metricRegistry.register(metricName, histogram);
          }
          return null;
        }
      });
    }
    return metrics.register(histogramName, histogram);
  }

  public static Gauge createGauge(MetricRegistry metrics, String name, final Gauge guage, final String pipelineName,
                                  final String pipelineRev) {
    final String gaugeName = metricName(name, GAUGE_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null && runningPipelines.contains(jmxNamePrefix)) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          String metricName = jmxNamePrefix  + gaugeName;
          if(!metricRegistry.getNames().contains(metricName)) {
            metricRegistry.register(metricName, guage);
          }
          return null;
        }
      });
    }
    return metrics.register(gaugeName, guage);
  }

  public static Counter getCounter(MetricRegistry metrics, String name) {
    return metrics.getCounters().get(metricName(name, COUNTER_SUFFIX));
  }

  public static ExtendedMeter getMeter(MetricRegistry metrics, String name) {
    return (ExtendedMeter) metrics.getMeters().get(metricName(name, METER_SUFFIX));
  }

  public static Histogram getHistogram(MetricRegistry metrics, String name) {
    return metrics.getHistograms().get(metricName(name, HISTOGRAM_M5_SUFFIX));
  }

  public static Timer getTimer(MetricRegistry metrics, String name) {
    return metrics.getTimers().get(metricName(name, TIMER_SUFFIX));
  }

  public static Gauge getGauge(MetricRegistry metrics, String name) {
    return metrics.getGauges().get(metricName(name, GAUGE_SUFFIX));
  }

  public static boolean removeGauge(MetricRegistry metrics, String name, final String pipelineName,
                                    final String pipelineRev) {
    final String gaugeName = metricName(name, GAUGE_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          metricRegistry.remove(jmxNamePrefix  + gaugeName);
          return null;
        }
      });
    }
    return metrics.remove(gaugeName);
  }

  public static boolean removeMeter(MetricRegistry metrics, String name, final String pipelineName,
                                    final String pipelineRev) {
    final String meterName = metricName(name, METER_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          metricRegistry.remove(jmxNamePrefix  + meterName);
          return null;
        }
      });
    }
    return metrics.remove(meterName);
  }

  public static boolean removeCounter(MetricRegistry metrics, String name, final String pipelineName,
                                      final String pipelineRev) {
    final String counterName = metricName(name, COUNTER_SUFFIX);
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    final MetricRegistry metricRegistry = sdcMetrics;
    if (metricRegistry != null) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          metricRegistry.remove(jmxNamePrefix  + counterName);
          return null;
        }
      });
    }
    return metrics.remove(counterName);
  }

  public static synchronized void registerJmxMetrics(MetricRegistry metrics) {
    sdcMetrics = metrics;
  }

  public static synchronized void registerPipeline(String pipelineName, String pipelineRev) {
    runningPipelines.add(jmxNamePrefix(pipelineName, pipelineRev));
  }

  public static synchronized void cleanUpJmxMetrics(final String pipelineName, final String pipelineRev) {
    final MetricRegistry metricRegistry = sdcMetrics;
    final String jmxNamePrefix = jmxNamePrefix(pipelineName, pipelineRev);
    runningPipelines.remove(jmxNamePrefix);
    if (metricRegistry != null) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          for (String name : metricRegistry.getNames()) {
            if (name.startsWith(jmxNamePrefix)) {
              metricRegistry.remove(name);
            }
          }
          return null;
        }
      });
    }
  }

  public static boolean resetCounter(MetricRegistry metrics, String name) {
    Counter counter = getCounter(metrics, name);
    boolean result = false;
    if(counter != null) {
      //there could be race condition with observer thread trying to update the counter. This should be ok.
      counter.dec(counter.getCount());
      result = true;
    }
    return result;
  }

}
