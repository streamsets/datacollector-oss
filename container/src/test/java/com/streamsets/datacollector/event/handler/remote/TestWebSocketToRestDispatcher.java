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
package com.streamsets.datacollector.event.handler.remote;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.execution.alerts.TestWebhookNotifier;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.tunneling.TunnelingRequest;
import com.streamsets.datacollector.tunneling.TunnelingResponse;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static com.streamsets.datacollector.event.handler.remote.WebSocketToRestDispatcher.AVAILABLE_APPS_ENDPOINT;
import static com.streamsets.datacollector.event.handler.remote.WebSocketToRestDispatcher.TUNNELING_CONNECT_ENDPOINT;

public class TestWebSocketToRestDispatcher {
  private Server server;
  private MockTunnelingWebSocketServlet mockTunnelingWebSocketServlet;

  @Before
  public void setUp() throws Exception {
    mockTunnelingWebSocketServlet = new MockTunnelingWebSocketServlet();
    MockServlet mockServlet = new MockServlet();

    ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    handler.addServlet(new ServletHolder(mockTunnelingWebSocketServlet), "/" + TUNNELING_CONNECT_ENDPOINT);
    handler.addServlet(new ServletHolder(mockServlet), "/" + AVAILABLE_APPS_ENDPOINT);
    handler.addServlet(new ServletHolder(mockServlet), "/rest/v1/pipeline");

    server = new Server(TestWebhookNotifier.getFreePort());
    server.setHandler(handler);
    server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  public Configuration getMockConfiguration(boolean enabled) {
    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.when(configuration.get(
        WebSocketToRestDispatcher.TUNNELING_ENABLED_CONFIG,
        WebSocketToRestDispatcher.TUNNELING_ENABLED_CONFIG_DEFAULT
    )).thenReturn(enabled);
    return configuration;
  }

  @Test
  public void testIsTunnelingEnabled() {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    DpmClientInfo dpmClientInfo = new DpmClientInfo() {
      @Override
      public String getDpmBaseUrl() {
        return server.getURI().toString();
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
    Mockito.when(runtimeInfo.getAttribute(Mockito.eq(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)))
        .thenReturn(dpmClientInfo);
    WebSocketToRestDispatcher webSocketToRestDispatcher = new WebSocketToRestDispatcher(
        getMockConfiguration(false),
        runtimeInfo,
        Mockito.mock(SafeScheduledExecutorService.class)
    );
    Assert.assertFalse(webSocketToRestDispatcher.isTunnelingEnabled());
  }

  @Test
  public void testRestDispatcher() throws IOException, InterruptedException {
    RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(mockRuntimeInfo.getOriginalHttpUrl())
        .thenReturn(StringUtils.stripEnd(server.getURI().toString(), "/"));
    DpmClientInfo dpmClientInfo = new DpmClientInfo() {
      @Override
      public String getDpmBaseUrl() {
        return server.getURI().toString();
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
    Mockito.when(mockRuntimeInfo.getAttribute(Mockito.eq(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)))
        .thenReturn(dpmClientInfo);
    WebSocketToRestDispatcher webSocketToRestDispatcher = new WebSocketToRestDispatcher(
        getMockConfiguration(true),
        mockRuntimeInfo,
        Mockito.mock(SafeScheduledExecutorService.class)
    );
    try {
      webSocketToRestDispatcher.runTask();

      Session wsServerSession = mockTunnelingWebSocketServlet.mockTunnelingWebSocket.session;

      // Test GET
      TunnelingRequest tRequest = new TunnelingRequest();
      tRequest.setId(UUID.randomUUID().toString());
      tRequest.setMethod("GET");
      tRequest.setPath("rest/v1/pipeline");
      tRequest.setQueryString("?param1=value1");
      tRequest.setHeaders(ImmutableMap.of("mockHeader", Collections.singletonList("mockHeaderValue")));
      wsServerSession.getRemote().sendBytes(ByteBuffer.wrap(ObjectMapperFactory.get().writeValueAsBytes(tRequest)));

      Thread.sleep(1000);

      TunnelingResponse tResponse = mockTunnelingWebSocketServlet.mockTunnelingWebSocket.lastMessage;
      Assert.assertNotNull(tResponse);
      Assert.assertEquals(200, tResponse.getStatus());
      Assert.assertEquals(tRequest.getId(), tResponse.getId());
      Assert.assertNotNull(tResponse.getPayload());

      // Test POST
      tRequest = new TunnelingRequest();
      tRequest.setId(UUID.randomUUID().toString());
      tRequest.setMethod("POST");
      tRequest.setPath("rest/v1/pipeline");
      tRequest.setQueryString("?param1=value1");
      tRequest.setHeaders(ImmutableMap.of("mockHeader", Collections.singletonList("mockHeaderValue")));
      tRequest.setPayload(ObjectMapperFactory.get().writeValueAsBytes(ImmutableMap.of("pipelineId", "pipeline")));
      wsServerSession.getRemote().sendBytes(ByteBuffer.wrap(ObjectMapperFactory.get().writeValueAsBytes(tRequest)));

      Thread.sleep(1000);

      tResponse = mockTunnelingWebSocketServlet.mockTunnelingWebSocket.lastMessage;
      Assert.assertNotNull(tResponse);
      Assert.assertEquals(200, tResponse.getStatus());
      Assert.assertEquals(tRequest.getId(), tResponse.getId());
      Assert.assertNotNull(tResponse.getPayload());

    } finally {
      webSocketToRestDispatcher.stopTask();
    }
  }
}
