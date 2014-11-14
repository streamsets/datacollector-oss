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

import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPipelineTask {

  @Test
  public void testPipelineAgentDelegation() {
    PipelineStoreTask store = Mockito.mock(PipelineStoreTask.class);
    WebServerTask webServer = Mockito.mock(WebServerTask.class);
    PipelineTask task = new PipelineTask(store, webServer);
    task.init();
    Mockito.verify(store, Mockito.times(1)).init();
    Mockito.verify(webServer, Mockito.times(1)).init();
    Mockito.verifyNoMoreInteractions(webServer);
    task.run();
    Mockito.verify(webServer, Mockito.times(1)).run();
    Mockito.verifyNoMoreInteractions(webServer);
    task.stop();
    Mockito.verify(webServer, Mockito.times(1)).stop();
    Mockito.verify(store, Mockito.times(1)).stop();
    Mockito.verifyNoMoreInteractions(webServer);
  }

  @Test
  public void testLatch() throws Exception {
    PipelineStoreTask store = Mockito.mock(PipelineStoreTask.class);
    WebServerTask webServer = Mockito.mock(WebServerTask.class);
    final PipelineTask task = new PipelineTask(store, webServer);
    task.init();
    long now = System.currentTimeMillis();
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          //NOP
        }
        task.stop();
      }
    }.start();
    task.run();
    task.waitWhileRunning();
    Assert.assertTrue(System.currentTimeMillis() - now >= 100);
  }

}
