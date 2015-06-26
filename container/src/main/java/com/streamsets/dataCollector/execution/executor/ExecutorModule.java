/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.executor;

import com.streamsets.dataCollector.execution.preview.Constants;
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
      configuration.get(Constants.PREVIEWER_THREAD_POOL_SIZE_KEY, Constants.PREVIEWER_THREAD_POOL_SIZE_DEFAULT), "preview");
  }

  @Provides @Singleton @Named("runnerExecutor")
  public SafeScheduledExecutorService provideRunnerExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(
      configuration.get(Constants.RUNNER_THREAD_POOL_SIZE_KEY, Constants.RUNNER_THREAD_POOL_SIZE_DEFAULT), "runner");
  }

  @Provides @Named("asyncExecutor")
  public SafeScheduledExecutorService provideAsyncExecutor(Configuration configuration) {
    return new SafeScheduledExecutorService(1, "asyncExecutor");
  }
}
