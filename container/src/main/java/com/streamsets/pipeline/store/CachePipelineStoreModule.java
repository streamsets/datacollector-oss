/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store;

import com.streamsets.dc.execution.store.CachePipelineStateStoreModule;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.stagelibrary.StageLibraryModule;
import com.streamsets.pipeline.store.impl.CachePipelineStoreTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = PipelineStoreTask.class, library = true, includes = {RuntimeModule.class, StageLibraryModule.class,
  CachePipelineStateStoreModule.class})
public class CachePipelineStoreModule {

  @Provides
  @Singleton
  public PipelineStoreTask provideStore(FilePipelineStoreTask store) {
    return new CachePipelineStoreTask(store);
  }

}
