/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.publicrestapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.event.dto.DisconnectedSsoCredentialsEvent;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.CallbackInfoJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DisconnectedSSOManager;
import com.streamsets.lib.security.http.DisconnectedSecurityInfo;
import com.streamsets.lib.security.http.PasswordHasher;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.File;
import java.util.Collections;
import java.util.UUID;

public class TestPublicClusterResource {

  @Test
  public void testPublicClusterResource() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Manager manager = Mockito.mock(Manager.class);
    PublicClusterResource publicClusterResource = Mockito.spy(new PublicClusterResource(manager, runtimeInfo));
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);
    Mockito.doNothing().when(publicClusterResource).updateSlaveCallbackInfo(Mockito.any());
    File testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    File authInfoFile = new File(testDir, DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_FILE);

    Configuration conf = new Configuration();
    conf.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(conf);

    DisconnectedSecurityInfo info = new DisconnectedSecurityInfo();
    info.addEntry(
        "admin@org",
        hasher.getPasswordHash("admin@org", "admin"),
        ImmutableList.of("datacollector:admin", "user"),
        Collections.<String>emptyList()
    );

    info.toJsonFile(authInfoFile);
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.toString());
    Response response = publicClusterResource.callbackWithResponse(Mockito.mock(CallbackInfoJson.class));
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    DisconnectedSsoCredentialsEvent disconnectedSsoCredentialsEvent = (DisconnectedSsoCredentialsEvent) response
        .getEntity();
    Assert.assertEquals(1, disconnectedSsoCredentialsEvent.getEntries().size());
  }

}
