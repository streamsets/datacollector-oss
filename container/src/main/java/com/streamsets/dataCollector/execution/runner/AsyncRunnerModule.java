/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.executor.ExecutorModule;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;

@Module(injects = Runner.class, library = true, includes = {StandaloneRunnerModule.class, ExecutorModule.class})
public class AsyncRunnerModule {

  @Provides
  public Runner provideRunner(StandaloneRunner runner, @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor) {
    return new AsyncRunner(runner, runnerExecutor);
  }
}
