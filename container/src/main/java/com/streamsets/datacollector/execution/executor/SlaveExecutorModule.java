/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.execution.executor;

import com.streamsets.datacollector.main.SlaveRuntimeModule;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Provides separate singleton instances of SafeScheduledExecutorService for running pipelines in cluster mode.
 */
@Module(injects = SafeScheduledExecutorService.class, library = true, includes = {SlaveRuntimeModule.class})
public class SlaveExecutorModule {

  @Provides
  @Singleton
  @Named("runnerExecutor")
  public SafeScheduledExecutorService provideRunnerExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      ExecutorModule.getRunnerSize(configuration),
      "runner"
    );
  }

  @Provides
  @Singleton
  @Named("runnerStopExecutor")
  public SafeScheduledExecutorService provideRunnerStopExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      ExecutorModule.getRunnerStopSize(configuration),
      "runnerStop"
    );

    // We don't explicitly set thread expiration for stop executor in slave as it's allowed to run only one pipeline.
  }

  @Provides
  @Singleton
  @Named("eventHandlerExecutor")
  public SafeScheduledExecutorService provideEventExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      ExecutorModule.getEventSize(configuration),
      "eventHandlerExecutor"
    );
  }

  @Provides
  @Singleton
  @Named("supportBundleExecutor")
  public SafeScheduledExecutorService provideSupportBundleExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      ExecutorModule.getBundleSize(configuration),
      "supportBundleExecutor"
    );
  }

  @Provides
  @Singleton
  ResourceManager provideResourceManager(Configuration configuration) {
    return new ResourceManager(configuration);
  }
}
