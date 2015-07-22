/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.standalone.dagger;

import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Named;

@Module(injects = Runner.class, library = true, complete = false)
public class StandaloneRunnerModule {

  private final String name;
  private final String rev;
  private final String user;

  private final ObjectGraph objectGraph;

  public StandaloneRunnerModule(String user, String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.objectGraph = objectGraph;
  }

  @Provides
  public StandaloneRunner provideRunner() {
    return new StandaloneRunner(user, name, rev, objectGraph);
  }

  @Provides
  public Runner provideAsyncRunner(StandaloneRunner runner,
                                        @Named("runnerExecutor") SafeScheduledExecutorService asyncExecutor) {
    return new AsyncRunner(runner, asyncExecutor);
  }
}
