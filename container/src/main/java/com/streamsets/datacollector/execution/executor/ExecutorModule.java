/**
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

import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Provides separate singleton instances of SafeScheduledExecutorService for previewing and running pipelines.
 * The thread pool size can be configured by setting the following properties in the sdc.properties file:
 * <ul>
 *   <li><code>preview.thread.pool.size</code></li>
 *   <li><code>runner.thread.pool.size</code></li>
 * </ul>
 * The default size for both the pools are 10.
 *
 */
@Module(injects = SafeScheduledExecutorService.class, library = true, includes = {RuntimeModule.class})
public class ExecutorModule {

  @Provides @Singleton @Named("previewExecutor")
  public SafeScheduledExecutorService providePreviewExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.PREVIEWER_THREAD_POOL_SIZE_KEY, ExecutorConstants.PREVIEWER_THREAD_POOL_SIZE_DEFAULT), "preview");
  }

  @Provides @Singleton @Named("runnerExecutor")
  public SafeScheduledExecutorService provideRunnerExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.RUNNER_THREAD_POOL_SIZE_KEY, ExecutorConstants.RUNNER_THREAD_POOL_SIZE_DEFAULT), "runner");
  }

  @Provides @Singleton @Named("managerExecutor")
  public SafeScheduledExecutorService provideManagerExecutor(Configuration configuration) {
    //thread used to evict runners from the cache in manager
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.MANAGER_EXECUTOR_THREAD_POOL_SIZE_KEY, ExecutorConstants.MANAGER_EXECUTOR_THREAD_POOL_SIZE_DEFAULT),
      "managerExecutor");
  }

  @Provides @Singleton @Named("eventHandlerExecutor")
  public SafeScheduledExecutorService provideEventExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.EVENT_EXECUTOR_THREAD_POOL_SIZE_KEY, ExecutorConstants.EVENT_EXECUTOR_THREAD_POOL_SIZE_DEFAULT),
      "eventHandlerExecutor");
  }

  @Provides @Singleton @Named("supportBundleExecutor")
  public SafeScheduledExecutorService provideSupportBundleExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.BUNDLE_EXECUTOR_THREAD_POOL_SIZE_KEY, ExecutorConstants.BUNDLE_EXECUTOR_THREAD_POOL_SIZE_DEFAULT),
      "supportBundleExecutor"
    );
  }

  @Provides @Singleton
  ResourceManager provideResourceManager(Configuration configuration) {
    return new ResourceManager(configuration);
  }
}
