/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.stagelibrary.StageLibraryModule;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreModule;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(library = true, includes = {RuntimeModule.class, PipelineStoreModule.class, StageLibraryModule.class})
public class ProdManagerModule {

  @Provides
  @Singleton
  public PipelineManager provideProdPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration
      , PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) {
    return new ProductionPipelineManagerTask(runtimeInfo, configuration, pipelineStore, stageLibrary);
  }

}
