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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPipelineAgent {

  @Test
  public void testPipelineAgentDelegation() {
    WebServer webServer = Mockito.mock(WebServer.class);
    PipelineAgent agent = new PipelineAgent(webServer);
    //shutting down up front to disable the latch
    agent.shutdown();
    agent.init();
    Mockito.verify(webServer, Mockito.times(1)).init();
    Mockito.verifyNoMoreInteractions(webServer);
    agent.run();
    Mockito.verify(webServer, Mockito.times(1)).start();
    Mockito.verifyNoMoreInteractions(webServer);
    agent.stop();
    Mockito.verify(webServer, Mockito.times(1)).stop();
    Mockito.verifyNoMoreInteractions(webServer);
  }

  @Test
  public void testLatch() throws Exception {
    WebServer webServer = Mockito.mock(WebServer.class);
    final PipelineAgent agent = new PipelineAgent(webServer);
    long now = System.currentTimeMillis();
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(50);
        } catch (InterruptedException ex) {
          //NOP
        }
        agent.shutdown();
      }
    }.start();
    agent.run();
    Assert.assertTrue(System.currentTimeMillis() - now >= 50);
  }

}
