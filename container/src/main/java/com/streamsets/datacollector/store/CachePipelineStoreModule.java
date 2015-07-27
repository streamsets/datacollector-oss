/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store;

import com.streamsets.datacollector.execution.store.CachePipelineStateStoreModule;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.stagelibrary.StageLibraryModule;
import com.streamsets.datacollector.store.impl.CachePipelineStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;

import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = PipelineStoreTask.class, library = true, includes = {RuntimeModule.class, StageLibraryModule.class,
  CachePipelineStateStoreModule.class, LockCacheModule.class})
public class CachePipelineStoreModule {

  @Provides
  @Singleton
  public PipelineStoreTask provideStore(FilePipelineStoreTask store, LockCache<String> lockCache) {
    return new CachePipelineStoreTask(store, lockCache);
  }

}
