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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.http.DataCollectorWebServerTask;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.CompositeTask;

import javax.inject.Inject;

public class PipelineTask extends CompositeTask {

  private final Manager manager;
  private final PipelineStoreTask pipelineStoreTask;
  private final StageLibraryTask stageLibraryTask;
  private final WebServerTask webServerTask;
  private final LineagePublisherTask lineagePublisherTask;
  private final SupportBundleManager supportBundleManager;
  private final CredentialStoresTask credentialStoresTask;

  @Inject
  public PipelineTask(
    StageLibraryTask library,
    PipelineStoreTask store,
    Manager manager,
    DataCollectorWebServerTask webServerTask,
    EventHandlerTask eventHandlerTask,
    LineagePublisherTask lineagePublisherTask,
    SupportBundleManager supportBundleManager,
    CredentialStoresTask credentialStoresTask
  ) {
    super(
      "pipelineNode",
        ImmutableList.of(
            library,
            lineagePublisherTask,
            credentialStoresTask,
            store,
            webServerTask,
            manager,
            eventHandlerTask,
            supportBundleManager
        ),
      true);
    this.webServerTask = webServerTask;
    this.stageLibraryTask = library;
    this.pipelineStoreTask = store;
    this.manager = manager;
    this.lineagePublisherTask = lineagePublisherTask;
    this.supportBundleManager = supportBundleManager;
    this.credentialStoresTask = credentialStoresTask;
  }

  public Manager getManager() {
    return manager;
  }
  public PipelineStoreTask getPipelineStoreTask() {
    return pipelineStoreTask;
  }
  public StageLibraryTask getStageLibraryTask() {
    return stageLibraryTask;
  }
  public WebServerTask getWebServerTask() {
    return webServerTask;
  }
  public LineagePublisherTask getLineagePublisherTask() {
    return lineagePublisherTask;
  }
  public SupportBundleManager getSupportBundleManager() {
    return supportBundleManager;
  }

  public CredentialStoresTask getCredentialStoresTask() {
    return credentialStoresTask;
  }

}
