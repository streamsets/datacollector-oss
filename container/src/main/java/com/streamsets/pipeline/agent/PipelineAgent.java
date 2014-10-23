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
package com.streamsets.pipeline.agent;

import com.streamsets.pipeline.http.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;

public class PipelineAgent implements Agent {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineAgent.class);

  private final WebServer webServer;
  private final CountDownLatch latch;

  @Inject
  public PipelineAgent(WebServer webServer) {
    this.webServer = webServer;
    latch = new CountDownLatch(1);
  }

  @Override
  public void init() {
    LOG.debug("Initializing");
    webServer.init();
    LOG.debug("Initialized");
  }

  @Override
  public void run() {
    LOG.debug("Starting");
    webServer.start();


    LOG.debug("Running");
    try {
      latch.await();
    } catch (InterruptedException ex) {

    }
  }

  public void shutdown() {
    LOG.debug("Shutting down");
    latch.countDown();
  }

  @Override
  public void stop() {
    LOG.debug("Stopping");
    webServer.stop();
    LOG.debug("Stopped");
  }
}
