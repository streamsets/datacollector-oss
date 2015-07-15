/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.provider.dagger;

import com.streamsets.dc.execution.manager.RunnerProvider;
import com.streamsets.dc.execution.runner.provider.SlaveRunnerProviderImpl;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of RunnerProvider.
 */
@Module(injects = RunnerProvider.class, library = true)
public class SlaveRunnerProviderModule {

  @Provides
  @Singleton
  public RunnerProvider provideRunnerProvider(SlaveRunnerProviderImpl runnerProvider) {
    return runnerProvider;
  }
}
