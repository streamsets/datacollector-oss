/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.ConfigConfigurationJson;
import com.streamsets.datacollector.restapi.bean.ConnectionConfigurationJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.RestClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestConnectionRetriever {

  @Test
  public void testGet() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "streamsets1.com");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getAppAuthToken()).thenReturn("appAuthToken");
    Mockito.when(runtimeInfo.getId()).thenReturn("componentId");
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);
    RestClient.Builder builder = Mockito.spy(RestClient.builder("http://streamsets.com"));
    RestClient restClient = Mockito.mock(RestClient.class);
    Mockito.doReturn(restClient).when(builder).build();
    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    Mockito.when(restClient.get()).thenReturn(response);
    Mockito.when(response.haveData()).thenReturn(true);
    Mockito.when(response.successful()).thenReturn(true);
    List<ConfigConfigurationJson> configs = new ArrayList<>();
    configs.add(new ConfigConfigurationJson("a", "A"));
    configs.add(new ConfigConfigurationJson("b", "B"));
    ConnectionConfigurationJson ccj = new ConnectionConfigurationJson("type1", "1", configs);
    Mockito.when(response.getData(ConnectionConfigurationJson.class)).thenReturn(ccj);
    ConnectionRetriever connectionRetriever = Mockito.spy(
        new FakeConnectionRetriever(configuration, runtimeInfo, builder)
    );
    ConfigInjector.Context context = Mockito.mock(ConfigInjector.Context.class);
    Mockito.when(context.getUser()).thenReturn("user1");
    ConnectionConfiguration cc = connectionRetriever.get("connId", context);

    Assert.assertEquals("type1", cc.getType());
    Assert.assertEquals(1, cc.getVersion());
    Assert.assertEquals(2, cc.getConfiguration().size());
    Mockito.verify(connectionRetriever).getRestClientBuilder("streamsets1.com/");
    Mockito.verify(builder).path("/connection/rest/v1/connection/connId/configs");
    Mockito.verify(builder).json(true);
    Mockito.verify(builder).csrf(true);
    Mockito.verify(builder).appAuthToken("appAuthToken");
    Mockito.verify(builder).componentId("componentId");
    Mockito.verify(builder).queryParam("user", "user1");
  }

  @Test
  public void testGetDpmDisabled() {
    Configuration configuration = new Configuration();
    configuration.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "streamsets1.com");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false); // DPM disabled
    ConnectionRetriever connectionRetriever = Mockito.spy(new ConnectionRetriever(configuration, runtimeInfo));
    ConfigInjector.Context context = Mockito.mock(ConfigInjector.Context.class);
    ConnectionConfiguration cc = connectionRetriever.get("connId", context);

    Assert.assertNull(cc);
    Mockito.verify(context).createIssue(CreationError.CREATION_1105);
    Mockito.verify(connectionRetriever, Mockito.times(0)).getRestClientBuilder(Mockito.anyString());
  }

  @Test
  public void testGetHttpError() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "streamsets1.com");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getAppAuthToken()).thenReturn("appAuthToken");
    Mockito.when(runtimeInfo.getId()).thenReturn("componentId");
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);
    RestClient.Builder builder = Mockito.spy(RestClient.builder("http://streamsets.com"));
    RestClient restClient = Mockito.mock(RestClient.class);
    Mockito.doReturn(restClient).when(builder).build();
    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    Mockito.when(restClient.get()).thenReturn(response);
    Mockito.when(response.haveData()).thenReturn(true);
    Mockito.when(response.successful()).thenReturn(false);  // HTTP Error
    Mockito.when(response.getError()).thenReturn(Collections.singletonMap("message", "some HTTP problem"));
    ConnectionRetriever connectionRetriever = new FakeConnectionRetriever(configuration, runtimeInfo, builder);
    ConfigInjector.Context context = Mockito.mock(ConfigInjector.Context.class);
    Mockito.when(context.getUser()).thenReturn("user1");
    ConnectionConfiguration cc = connectionRetriever.get("connId", context);

    Assert.assertNull(cc);
    ArgumentCaptor<String> argCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(context).createIssue(Mockito.eq(CreationError.CREATION_1104), argCaptor.capture());
    Assert.assertTrue(argCaptor.getValue().contains("some HTTP problem"));
  }

  @Test
  public void testGetIOException() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "streamsets1.com");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getAppAuthToken()).thenReturn("appAuthToken");
    Mockito.when(runtimeInfo.getId()).thenReturn("componentId");
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);
    RestClient.Builder builder = Mockito.spy(RestClient.builder("http://streamsets.com"));
    RestClient restClient = Mockito.mock(RestClient.class);
    Mockito.doReturn(restClient).when(builder).build();
    IOException ioe = new IOException("some IO problem");
    Mockito.when(restClient.get()).thenThrow(ioe); // IOException
    ConnectionRetriever connectionRetriever = new FakeConnectionRetriever(configuration, runtimeInfo, builder);
    ConfigInjector.Context context = Mockito.mock(ConfigInjector.Context.class);
    Mockito.when(context.getUser()).thenReturn("user1");
    ConnectionConfiguration cc = connectionRetriever.get("connId", context);

    Assert.assertNull(cc);
    Mockito.verify(context).createIssue(
        CreationError.CREATION_1104,
        ioe.getMessage(),
        ioe
    );
  }

  private static class FakeConnectionRetriever extends ConnectionRetriever {

    private final RestClient.Builder builder;

    public FakeConnectionRetriever(Configuration configuration, RuntimeInfo runtimeInfo, RestClient.Builder builder) {
      super(configuration, runtimeInfo);
      this.builder = builder;
    }

    @Override
    protected RestClient.Builder getRestClientBuilder(String schBaseUrl) {
      return builder;
    }
  }
}
