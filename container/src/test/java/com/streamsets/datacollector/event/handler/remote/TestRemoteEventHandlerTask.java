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
package com.streamsets.datacollector.event.handler.remote;

import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DisconnectedSSOManager;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.UUID;

public class TestRemoteEventHandlerTask {

  @Test
  public void testDisconnectedSsoCredentialsDataStore() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());

    RemoteDataCollector remoteDataCollector = Mockito.mock(RemoteDataCollector.class);
    EventClient eventSenderReceiver = Mockito.mock(EventClient.class);
    SafeScheduledExecutorService executorService = Mockito.mock(SafeScheduledExecutorService.class);
    StageLibraryTask stageLibrary = Mockito.mock(StageLibraryTask.class);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());
    Configuration conf = new Configuration();
    RemoteEventHandlerTask task = new RemoteEventHandlerTask(remoteDataCollector,
        eventSenderReceiver,
        executorService,
        stageLibrary,
        runtimeInfo,
        conf
    );

    Assert.assertEquals(
        new File(testDir, DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_FILE),
        task.getDisconnectedSsoCredentialsDataStore().getFile()
    );
  }

}
