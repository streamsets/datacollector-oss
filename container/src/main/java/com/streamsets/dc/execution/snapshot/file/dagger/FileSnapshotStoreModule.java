/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.snapshot.file.dagger;

import com.streamsets.dc.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.pipeline.main.RuntimeModule;

import com.streamsets.pipeline.main.RuntimeInfo;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of FileSnapshotStore
 */
@Module(injects = FileSnapshotStore.class, library = true, includes = {RuntimeModule.class})
public class FileSnapshotStoreModule {

  @Provides @Singleton
  public FileSnapshotStore provideSnapshotStore(RuntimeInfo runtimeInfo) {
    return new FileSnapshotStore(runtimeInfo);
  }

}
