/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.cluster.dagger;

import com.streamsets.dc.execution.runner.cluster.ClusterRunner;
import com.streamsets.dc.execution.runner.common.AsyncRunner;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Named;

@Module(injects = AsyncRunner.class, library = true, complete = false)
public class ClusterRunnerModule {

  private final String name;
  private final String rev;
  private final String user;

  private final ObjectGraph objectGraph;

  public ClusterRunnerModule(String user, String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.objectGraph = objectGraph;
  }

  @Provides
  public ClusterRunner provideRunner() {
    return new ClusterRunner(user, name, rev, objectGraph);
  }

  @Provides
  public AsyncRunner provideAsyncRunner(ClusterRunner runner,
                                        @Named("asyncExecutor") SafeScheduledExecutorService asyncExecutor) {
    return new AsyncRunner(runner, asyncExecutor);
  }
}
