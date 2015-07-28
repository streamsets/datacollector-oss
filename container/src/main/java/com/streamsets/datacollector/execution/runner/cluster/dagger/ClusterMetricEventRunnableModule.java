/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.cluster.dagger;

import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.cluster.SlaveCallbackManager;
import com.streamsets.datacollector.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = {MetricsEventRunnable.class}, library = true, complete = false)
public class ClusterMetricEventRunnableModule {

  private final String name;
  private final String rev;

  public ClusterMetricEventRunnableModule(String name, String rev) {
    this.name = name;
    this.rev = rev;
  }

  @Provides @Singleton
  public MetricsEventRunnable provideMetricsEventRunnable(Configuration configuration,
                                                          PipelineStateStore pipelineStateStore,
                                                          EventListenerManager eventListenerManager,
                                                          SlaveCallbackManager slaveCallbackManager) {
    return new MetricsEventRunnable(name, rev, configuration, pipelineStateStore, null, eventListenerManager, null,
      slaveCallbackManager);
  }

}
