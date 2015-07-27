/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;
import java.lang.management.ManagementFactory;

@Module(library = true)
public class MetricsModule {

  @Provides @Singleton MetricRegistry provideMetrics() {
    MetricRegistry metrics = new MetricRegistry();
    metrics.register("jvm.memory", new MemoryUsageGaugeSet());
    metrics.register("jvm.garbage", new GarbageCollectorMetricSet());
    metrics.register("jvm.threads", new ThreadStatesGaugeSet());
    metrics.register("jvm.files", new FileDescriptorRatioGauge());
    metrics.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    return metrics;
  }

}
