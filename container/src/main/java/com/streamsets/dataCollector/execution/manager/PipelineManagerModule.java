/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager;

import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.executor.ExecutorModule;
import com.streamsets.dataCollector.execution.preview.PreviewerProviderModule;
import com.streamsets.dataCollector.execution.runner.RunnerProviderModule;
import com.streamsets.dataCollector.execution.store.PipelineStateStoreModule;
import com.streamsets.pipeline.store.PipelineStoreModule;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of Manager.
 */
@Module(library = true,
  includes = {PipelineStateStoreModule.class, PipelineStoreModule.class, ExecutorModule.class,
    PreviewerProviderModule.class, RunnerProviderModule.class})
public class PipelineManagerModule {

  @Provides @Singleton
  public Manager provideManager() {
    ObjectGraph objectGraph = ObjectGraph.create(PipelineStateStoreModule.class, PipelineStoreModule.class,
      ExecutorModule.class, PreviewerProviderModule.class, RunnerProviderModule.class);
    PipelineManager pipelineManager = new PipelineManager(objectGraph);
    return pipelineManager;
  }
}
