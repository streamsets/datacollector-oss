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
package com.streamsets.datacollector.execution.manager.slave.dagger;

import com.streamsets.datacollector.activation.ActivationModule;
import com.streamsets.datacollector.antennadoctor.AntennaDoctorModule;
import com.streamsets.datacollector.blobstore.BlobStoreModule;
import com.streamsets.datacollector.credential.CredentialStoresModule;
import com.streamsets.datacollector.execution.executor.SlaveExecutorModule;
import com.streamsets.datacollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.datacollector.execution.runner.provider.dagger.SlaveRunnerProviderModule;
import com.streamsets.datacollector.execution.snapshot.cache.dagger.SlaveCacheSnapshotStoreModule;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStoreModule;
import com.streamsets.datacollector.lineage.LineageModule;
import com.streamsets.datacollector.store.SlaveAclStoreModule;
import com.streamsets.datacollector.store.SlavePipelineStoreModule;

import com.streamsets.datacollector.usagestats.StatsCollectorModule;
import dagger.Module;

/**
 * Provides a singleton instance of Manager.
 */
@Module(
  library = true,
  injects = {SlavePipelineManager.class},
  includes = {
    ActivationModule.class,
    SlavePipelineStateStoreModule.class,
    SlavePipelineStoreModule.class,
    SlaveAclStoreModule.class,
    SlaveExecutorModule.class,
    SlaveRunnerProviderModule.class,
    SlaveCacheSnapshotStoreModule.class,
    LineageModule.class,
    BlobStoreModule.class,
    CredentialStoresModule.class,
    StatsCollectorModule.class,
    AntennaDoctorModule.class
  })
public class SlavePipelineManagerModule {

}
