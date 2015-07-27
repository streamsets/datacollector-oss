/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store;

import com.streamsets.datacollector.execution.store.SlavePipelineStateStoreModule;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.stagelibrary.StageLibraryModule;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.store.impl.SlavePipelineStoreTask;

import com.streamsets.datacollector.util.LockCacheModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = PipelineStoreTask.class, library = true, includes = {RuntimeModule.class, StageLibraryModule.class,
  SlavePipelineStateStoreModule.class, LockCacheModule.class})
public class SlavePipelineStoreModule {

  @Provides
  @Singleton
  public PipelineStoreTask provideStore(FilePipelineStoreTask store) {
    return new SlavePipelineStoreTask(store);
  }

}
