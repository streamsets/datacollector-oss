/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.executor;

import com.streamsets.dc.execution.common.ExecutorConstants;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.util.Configuration;
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

  @Provides @Singleton @Named("asyncExecutor")
  public SafeScheduledExecutorService provideAsyncExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.ASYNC_EXECUTOR_THREAD_POOL_SIZE_KEY, ExecutorConstants.ASYNC_EXECUTOR_THREAD_POOL_SIZE_DEFAULT),
      "asyncExecutor");
  }

  @Provides @Singleton @Named("managerExecutor")
  public SafeScheduledExecutorService provideManagerExecutor(Configuration configuration) {
    //thread used to evict runners from the cache in manager
    return new SafeScheduledExecutorService(
      configuration.get(ExecutorConstants.MANAGER_EXECUTOR_THREAD_POOL_SIZE_KEY, ExecutorConstants.MANAGER_EXECUTOR_THREAD_POOL_SIZE_DEFAULT),
      "managerExecutor");
  }
}
