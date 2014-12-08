/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.metrics;

import com.codahale.metrics.MetricRegistry;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(library = true)
public class MetricsModule {

  @Provides @Singleton MetricRegistry provideMetrics() {
    return new MetricRegistry();
  }


}
