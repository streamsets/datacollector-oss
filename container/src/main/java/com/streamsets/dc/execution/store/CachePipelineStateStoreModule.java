/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.store;

import com.streamsets.dc.execution.PipelineStateStore;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of FileSnapshotStore
 */
@Module(injects = PipelineStateStore.class, library = true, includes = {RuntimeModule.class})
public class CachePipelineStateStoreModule {

  @Provides @Singleton
  public PipelineStateStore providePipelineStateStore(RuntimeInfo runtimeInfo, Configuration configuration) {
    return new CachePipelineStateStore(new FilePipelineStateStore(runtimeInfo, configuration));
  }
}
