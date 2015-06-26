/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.executor.ExecutorModule;
import com.streamsets.dataCollector.execution.store.PipelineStateStoreModule;
import com.streamsets.pipeline.store.PipelineStoreModule;
import dagger.Module;
import dagger.Provides;

@Module(library = true, includes = {ExecutorModule.class, PipelineStoreModule.class,
  PipelineStateStoreModule.class})
public class ClusterRunnerModule {

  @Provides
  public Runner provideRunner() {
    return new ClusterRunner();
  }
}