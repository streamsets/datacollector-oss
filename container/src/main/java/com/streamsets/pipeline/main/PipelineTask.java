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
package com.streamsets.pipeline.main;

import com.streamsets.pipeline.http.WebServer;
import com.streamsets.pipeline.store.PipelineStore;
import com.streamsets.pipeline.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class PipelineTask extends AbstractTask {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTask.class);

  private final PipelineStore store;
  private final WebServer webServer;

  @Inject
  public PipelineTask(PipelineStore store, WebServer webServer) {
    super("pipeline");
    this.store = store;
    this.webServer = webServer;
  }

  @Override
  protected void initTask() {
    store.init();
    webServer.init();
  }

  @Override
  protected void runTask() {
    webServer.start();
  }

  @Override
  protected void stopTask() {
    try {
      webServer.stop();
    } catch (RuntimeException ex) {
      LOG.warn("Error while stopping the WebServer: {}", ex.getMessage(), ex);
    }
    try {
      store.destroy();
    } catch (RuntimeException ex) {
      LOG.warn("Error while destroying the PipelineStore: {}", ex.getMessage(), ex);
    }
  }
}
