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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.client.impl.MovedDpmJerseyClientFilter;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.ConnectionConfigurationJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import org.glassfish.jersey.client.ClientConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestConnectionRetriever {

  @Test
  public void testRetriever() throws Exception {
    DpmClientInfo dpmClientInfo = new DpmClientInfo() {
      @Override
      public String getDpmBaseUrl() {
        return "http://streamsets1.com/";
      }

      @Override
      public Map<String, String> getHeaders() {
        return ImmutableMap.of(
            SSOConstants.X_APP_COMPONENT_ID, "componentId",
            SSOConstants.X_APP_AUTH_TOKEN, "appAuthToken"
        );
      }

      @Override
      public void setDpmBaseUrl(String dpmBaseUrl) {

      }
    };
    Configuration configuration = new Configuration();
    configuration.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "streamsets1.com");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getAttribute(Mockito.eq(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)))
        .thenReturn(dpmClientInfo);
    Mockito.when(runtimeInfo.getId()).thenReturn("componentId");
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);

    Client client = Mockito.mock(Client.class);
    WebTarget target = Mockito.mock(WebTarget.class);
    Mockito.when(client.target(Mockito.eq("http://streamsets1.com/connection/rest/v1/connection/connId/configs")))
        .thenReturn(target);
    WebTarget target1 = Mockito.mock(WebTarget.class);
    Mockito.when(target.queryParam(Mockito.eq("user"), Mockito.eq("user1"))).thenReturn(target1);
    target = target1;
    Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);
    Mockito.when(target.request()).thenReturn(builder);
    Response response = Mockito.mock(Response.class);
    Mockito.when(builder.get()).thenReturn(response);
    Mockito.when(response.getStatusInfo()).thenReturn(Response.Status.OK);
    ConnectionConfigurationJson json = Mockito.mock(ConnectionConfigurationJson.class);
    Mockito.when(response.readEntity(Mockito.eq(ConnectionConfigurationJson.class))).thenReturn(json);
    ConnectionConfiguration conn = Mockito.mock(ConnectionConfiguration.class);
    Mockito.when(json.getConnectionConfiguration()).thenReturn(conn);

    ConnectionRetriever retriever = new ConnectionRetriever(configuration, runtimeInfo);

    // verify configuration

    ClientConfig cc = retriever.getClientConfig();
    Set<Class> instances = cc.getInstances().stream().map(i -> i.getClass()).collect(Collectors.toSet());
    Assert.assertTrue(instances.contains(MovedDpmJerseyClientFilter.class));

    // REST call

    retriever = Mockito.spy(retriever);
    Mockito.doReturn(client).when(retriever).getClient();

    ConfigInjector.Context context = Mockito.mock(ConfigInjector.Context.class);
    Mockito.when(context.getUser()).thenReturn("user1");

    //OK

    Assert.assertEquals(conn, retriever.get("connId", context));

    Mockito.verify(builder, Mockito.times(1)).header(
        Mockito.eq(SSOConstants.X_APP_COMPONENT_ID),
        Mockito.eq("componentId")
    );
    Mockito.verify(builder, Mockito.times(1)).header(
        Mockito.eq(SSOConstants.X_APP_AUTH_TOKEN),
        Mockito.eq("appAuthToken")
    );
    Mockito.verify(builder, Mockito.times(1)).header(
        Mockito.eq(SSOConstants.X_REST_CALL),
        Mockito.eq(SSOConstants.SDC_COMPONENT_NAME)
    );
    Mockito.verify(context, Mockito.times(1)).getUser();
    Mockito.verifyNoMoreInteractions(context);

    //NOT OK
    Mockito.when(response.getStatusInfo()).thenReturn(Response.Status.FORBIDDEN);
    Assert.assertNull(retriever.get("connId", context));
    Mockito.verify(context, Mockito.times(1)).createIssue(
        Mockito.eq(CreationError.CREATION_1104),
        Mockito.eq(Response.Status.FORBIDDEN.getStatusCode()),
        Mockito.anyString()
    );

    // dpm disabled
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false);
    Assert.assertNull(retriever.get("connId", context));
    Mockito.verify(context, Mockito.times(1)).createIssue(
        Mockito.eq(CreationError.CREATION_1105)
    );


  }

}
