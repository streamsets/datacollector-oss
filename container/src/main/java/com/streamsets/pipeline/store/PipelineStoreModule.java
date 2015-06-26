/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store;

import com.streamsets.dataCollector.execution.store.PipelineStateStoreModule;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.stagelibrary.StageLibraryModule;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(library = true, includes = {RuntimeModule.class, StageLibraryModule.class,
  PipelineStateStoreModule.class})
public class PipelineStoreModule {

  @Provides
  @Singleton
  public PipelineStoreTask provideStore(FilePipelineStoreTask store) {
    return store;
  }

}
