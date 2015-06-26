/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.dataCollector.execution.manager.RunnerProvider;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of RunnerProvider.
 */
@Module(injects = RunnerProvider.class, library = true)
public class RunnerProviderModule {

  @Provides
  @Singleton
  public RunnerProvider provideRunnerProvider(RunnerProviderImpl runnerProvider) {
    return runnerProvider;
  }
}
