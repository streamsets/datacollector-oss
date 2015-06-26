/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot;

import com.streamsets.dataCollector.execution.SnapshotStore;
import com.streamsets.pipeline.main.RuntimeModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of FileSnapshotStore
 */
@Module(injects = SnapshotStore.class, library = true, includes = {RuntimeModule.class})
public class FileSnapshotStoreModule {

  @Provides @Singleton
  public SnapshotStore provideSnapshotStore(CacheSnapshotStore snapshotStore) {
    return snapshotStore;
  }

}
