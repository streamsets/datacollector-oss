/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.api.impl.Utils;
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
public class PipelineManagerModule {

  @Provides
  @Singleton
  public PipelineManager provideProdPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration
      , PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) {
    PipelineManager manager;
    String runtimeMode = configuration.get("sdc.runtime.mode", "standalone");
    switch (runtimeMode) {
      case "standalone":
      case "slave":
        manager = new StandalonePipelineManagerTask(runtimeInfo, configuration, pipelineStore, stageLibrary);
        break;
      case "cluster":
        manager = new ClusterPipelineManager(runtimeInfo, configuration, pipelineStore, stageLibrary);
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid runtime mode '{}'", runtimeMode));
    }
    return manager;
  }

}
