/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.runner.edge.dagger;

import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.edge.EdgeRunner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Named;

@Module(injects = Runner.class, library = true, complete = false)
public class EdgeRunnerModule {

  private final String name;
  private final String rev;
  private final ObjectGraph objectGraph;

  public EdgeRunnerModule(String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.objectGraph = objectGraph;
  }

  @Provides
  public EdgeRunner provideEdgeRunner(
      Configuration configuration,
      RuntimeInfo runtimeInfo,
      EventListenerManager eventListenerManager
  ) {
    return new EdgeRunner(name, rev, objectGraph);
  }

  @Provides
  public Runner provideAsyncRunner(
      EdgeRunner runner,
      @Named("runnerExecutor") SafeScheduledExecutorService asyncExecutor,
      @Named("runnerStopExecutor") SafeScheduledExecutorService asyncStopExecutor
  ) {
    return new AsyncRunner(runner, asyncExecutor, asyncStopExecutor);
  }
}
