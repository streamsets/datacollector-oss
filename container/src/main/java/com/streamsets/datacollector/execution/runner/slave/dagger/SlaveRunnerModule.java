/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.slave.dagger;

import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.slave.SlaveStandaloneRunner;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Named;

@Module(injects = Runner.class, library = true, complete = false)
public class SlaveRunnerModule {

  private final String name;
  private final String rev;
  private final String user;

  private final ObjectGraph objectGraph;

  public SlaveRunnerModule(String user, String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.objectGraph = objectGraph;
  }

  @Provides
  public SlaveStandaloneRunner provideStandaloneRunner(Configuration configuration, RuntimeInfo runtimeInfo) {
    return new SlaveStandaloneRunner(new StandaloneRunner(user, name, rev, objectGraph), configuration, runtimeInfo);
  }

  @Provides
  public Runner provideAsyncRunner(SlaveStandaloneRunner runner,
                                        @Named("runnerExecutor") SafeScheduledExecutorService asyncExecutor) {
    return new AsyncRunner(runner, asyncExecutor);
  }
}
