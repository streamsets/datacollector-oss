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
