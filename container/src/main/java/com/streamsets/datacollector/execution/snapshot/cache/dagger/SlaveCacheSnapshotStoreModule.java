/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.snapshot.cache.dagger;

import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.snapshot.cache.CacheSnapshotStore;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.execution.snapshot.file.dagger.SlaveFileSnapshotStoreModule;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of FileSnapshotStore
 */
@Module(injects = SnapshotStore.class, library = true, includes = {SlaveFileSnapshotStoreModule.class,
  LockCacheModule.class})
public class SlaveCacheSnapshotStoreModule {

  @Provides
  @Singleton
  public SnapshotStore provideSnapshotStore(FileSnapshotStore fileSnapshotStore,
     LockCache<String> lockCache) {
    return new CacheSnapshotStore(fileSnapshotStore, lockCache);
  }

}
