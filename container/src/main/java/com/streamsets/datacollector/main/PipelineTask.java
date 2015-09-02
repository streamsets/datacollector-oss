/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.CompositeTask;

import javax.inject.Inject;

public class PipelineTask extends CompositeTask {

  private final Manager manager;
  private final PipelineStoreTask pipelineStoreTask;
  private final StageLibraryTask stageLibraryTask;
  private final WebServerTask webServerTask;

  @Inject
  public PipelineTask(StageLibraryTask library, PipelineStoreTask store, Manager manager,
      WebServerTask webServerTask) {
    super("pipelineNode", ImmutableList.of(library, store, manager, webServerTask),
      true);
    this.webServerTask = webServerTask;
    this.stageLibraryTask = library;
    this.pipelineStoreTask = store;
    this.manager = manager;
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

}
