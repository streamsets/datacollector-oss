/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot.cache.dagger;

import com.streamsets.dataCollector.execution.SnapshotStore;
import com.streamsets.dataCollector.execution.snapshot.cache.CacheSnapshotStore;
import com.streamsets.dataCollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.dataCollector.execution.snapshot.file.dagger.FileSnapshotStoreModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of FileSnapshotStore
 */
@Module(injects = SnapshotStore.class, library = true, includes = {FileSnapshotStoreModule.class})
public class CacheSnapshotStoreModule {

  @Provides
  @Singleton
  public SnapshotStore provideSnapshotStore(FileSnapshotStore fileSnapshotStore) {
    return new CacheSnapshotStore(fileSnapshotStore);
  }

}
