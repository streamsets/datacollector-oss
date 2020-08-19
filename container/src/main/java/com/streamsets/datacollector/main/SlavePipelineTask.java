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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.antennadoctor.AntennaDoctor;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.aster.EntitlementSyncTask;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.http.SlaveWebServerTask;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;

import javax.inject.Inject;

public class SlavePipelineTask extends PipelineTask {

  @Inject
  public SlavePipelineTask(
    StageLibraryTask library,
    PipelineStoreTask store,
    Manager manager,
    SlaveWebServerTask slaveWebServerTask,
    EventHandlerTask eventHandlerTask,
    LineagePublisherTask lineagePublisherTask,
    SupportBundleManager supportBundleManager,
    BlobStoreTask blobStoreTask,
    CredentialStoresTask credentialStoresTask,
    StatsCollector statsCollector,
    AntennaDoctor antennaDoctor,
    EntitlementSyncTask entitlementSyncTask
  ) {
    super(
      library,
      store,
      manager,
      slaveWebServerTask,
      eventHandlerTask,
      lineagePublisherTask,
      supportBundleManager,
      blobStoreTask,
      credentialStoresTask,
      statsCollector,
      antennaDoctor,
      entitlementSyncTask
    );
  }
}
