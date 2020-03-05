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

import com.streamsets.datacollector.credential.CredentialStoresModule;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStoreModule;
import com.streamsets.datacollector.main.SlaveRuntimeModule;
import com.streamsets.datacollector.stagelibrary.SlaveStageLibraryModule;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.store.impl.SlavePipelineStoreTask;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = PipelineStoreTask.class, library = true, includes = {
    SlaveRuntimeModule.class,
    SlaveStageLibraryModule.class,
    SlavePipelineStateStoreModule.class,
    LockCacheModule.class,
    CredentialStoresModule.class
})
public class SlavePipelineStoreModule {

  @Provides
  @Singleton
  public PipelineStoreTask provideStore(FilePipelineStoreTask filePipelineStoreTask) {
    return new SlavePipelineStoreTask(filePipelineStoreTask);
  }

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

}
