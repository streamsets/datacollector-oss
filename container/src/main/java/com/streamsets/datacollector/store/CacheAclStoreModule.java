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
package com.streamsets.datacollector.store;

import com.streamsets.datacollector.activation.ActivationModule;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.store.impl.CacheAclStoreTask;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = AclStoreTask.class, library = true, includes = {
        ActivationModule.class,
        CachePipelineStoreModule.class,
        LockCacheModule.class})
public class CacheAclStoreModule {

  @Provides
  @Singleton
  public AclStoreTask provideAclStore(
      FileAclStoreTask aclStore,
      PipelineStoreTask pipelineStore,
      LockCache<String> lockCache,
      UserGroupManager userGroupManager
  ) {
    return new CacheAclStoreTask(aclStore, pipelineStore, lockCache, userGroupManager);
  }

}
