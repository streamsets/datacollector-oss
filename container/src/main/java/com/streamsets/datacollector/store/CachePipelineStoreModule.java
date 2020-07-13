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
import com.streamsets.datacollector.credential.CredentialStoresModule;
import com.streamsets.datacollector.execution.executor.ExecutorModule;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.execution.store.CachePipelineStateStoreModule;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.stagelibrary.StageLibraryModule;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.impl.CachePipelineStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;

import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.usagestats.StatsCollectorModule;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(
    injects = PipelineStoreTask.class,
    library = true,
    includes = {
        ActivationModule.class,
        RuntimeModule.class,
        ExecutorModule.class,
        StageLibraryModule.class,
        CachePipelineStateStoreModule.class,
        LockCacheModule.class,
        CredentialStoresModule.class,
        StatsCollectorModule.class
    }
)
public class CachePipelineStoreModule {

  @Provides
  public PipelineCredentialHandler provideEncryptingPipelineCredentialsHandler(
      StageLibraryTask stageLibraryTask,
      CredentialStoresTask credentialStoresTask,
      Configuration configuration
  ) {
    return PipelineCredentialHandler.getEncrypter(
        stageLibraryTask,
        credentialStoresTask,
        configuration
    );
  }

  @Provides
  @Singleton
  public PipelineStoreTask provideStore(
      FilePipelineStoreTask store,
      LockCache<String> lockCache,
      StatsCollector statsCollector
  ) {
    return new StatsCollectorPipelineStoreTask(new CachePipelineStoreTask(store, lockCache), statsCollector);
  }

}
