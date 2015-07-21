/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.cluster.dagger;

import com.streamsets.dc.execution.EventListenerManager;
import com.streamsets.dc.execution.runner.cluster.ClusterRunner;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = ClusterRunner.class, library = true, complete = false)
public class ClusterRunnerInjectorModule {
  @Provides
  @Singleton
  public EventListenerManager provideEventListenerManager() {
    return new EventListenerManager();
  }
}
