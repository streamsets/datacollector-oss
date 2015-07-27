/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.standalone.dagger;

import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;

import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;
import javax.ws.rs.ext.Provider;

@Module(injects = StandaloneRunner.class, library = true, complete = false)
public class StandaloneRunnerInjectorModule {
  @Provides @Singleton
  public EventListenerManager provideEventListenerManager() {
    return new EventListenerManager();
  }
}
