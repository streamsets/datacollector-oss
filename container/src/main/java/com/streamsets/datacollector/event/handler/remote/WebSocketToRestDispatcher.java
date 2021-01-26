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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.tunneling.TunnelingRequest;
import com.streamsets.datacollector.tunneling.TunnelingResponse;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.api.extensions.ExtensionConfig;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@WebSocket
public class WebSocketToRestDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketToRestDispatcher.class);
  static final String TUNNELING_ENABLED_CONFIG = "dpm.tunneling.enabled";
  static final boolean TUNNELING_ENABLED_CONFIG_DEFAULT = true;
  static final String TUNNELING_PING_INTERVAL_CONFIG = "dpm.tunneling.ping.interval.secs";
  static final long TUNNELING_PING_INTERVAL_CONFIG_DEFAULT = 120;
  static final String TUNNELING_APP_NAME = "tunneling";
  static final String AVAILABLE_APPS_ENDPOINT = "rest/v1/availableApps";
  static final String TUNNELING_CONNECT_ENDPOINT = "tunneling/rest/v1/connect";
  private static final String PER_MESSAGE_DEFLATE = "permessage-deflate";
  private static final String PING_MESSAGE = "ping";
  private final Configuration conf;
  private final RuntimeInfo runtimeInfo;
  private final SafeScheduledExecutorService executorService;
  private String webSocketConnectUrl;
  private WebSocketClient webSocketClient;
  private Client httpClient;
  private Session wsSession = null;

  public WebSocketToRestDispatcher(
      Configuration configuration,
      RuntimeInfo runtimeInfo,
      SafeScheduledExecutorService executorService
  ) {
    this.conf = configuration;
    this.runtimeInfo = runtimeInfo;
    this.executorService = executorService;
  }

  public void runTask() {
    String controlHubUrl = RemoteSSOService.getValidURL(conf.get(
        RemoteSSOService.DPM_BASE_URL_CONFIG,
        RemoteSSOService.DPM_BASE_URL_DEFAULT
    ));
    if (isTunnelingEnabled(controlHubUrl)) {
      try {
        this.webSocketConnectUrl = controlHubUrl.replaceFirst("http", "ws") + TUNNELING_CONNECT_ENDPOINT;
        this.httpClient = ClientBuilder.newClient()
            .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);

        boolean connected = this.connectToControlHubTunnelingApp();
        if (connected) {
          // Keep the WebSocket Connection open by sending a ping message every two minutes.
          long interval = conf.get(TUNNELING_PING_INTERVAL_CONFIG, TUNNELING_PING_INTERVAL_CONFIG_DEFAULT);
          executorService.scheduleAtFixedRate(this::sendPing, 120, interval, TimeUnit.SECONDS);

          // Disable static web content when connected to the latest Control Hub instance
          runtimeInfo.setStaticWebDisabled(true);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  private boolean connectToControlHubTunnelingApp() {
    try {
      if (this.webSocketClient != null) {
        this.webSocketClient.stop();
      }

      URI webSocketUri = new URI(webSocketConnectUrl);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      request.addExtensions(ExtensionConfig.parse(PER_MESSAGE_DEFLATE)); // for message compression
      request.setHeader(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
      request.setHeader(SSOConstants.X_APP_AUTH_TOKEN, runtimeInfo.getAppAuthToken());
      request.setHeader(SSOConstants.X_APP_COMPONENT_ID, runtimeInfo.getId());

      this.webSocketClient = new WebSocketClient();
      this.webSocketClient.start();
      this.webSocketClient.connect(this, webSocketUri, request).get();
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    LOG.debug("onConnect: {}", session);
    this.wsSession = session;
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    LOG.debug("onClose: {}: {}", statusCode, reason);
  }

  @OnWebSocketError
  public void onError(Throwable cause) {
    LOG.error("onError: {}", cause.getMessage(), cause);
    // Reconnect on Error
    this.connectToControlHubTunnelingApp();
  }

  @OnWebSocketMessage
  public void onMessage(String message) {
    LOG.debug("onMessage string: {}", message);
  }

  @OnWebSocketMessage
  public void onMessage(byte[] payload, int offset, int len) {
    try {
      TunnelingRequest tRequest = ObjectMapperFactory.get()
          .readValue(payload, offset, len, TunnelingRequest.class);
      if (tRequest != null && tRequest.getPath() != null) {
        Response response = null;
        try {
          response = proxyRequest(tRequest);
          if (response != null) {
            Object data;

            String contentType = response.getHeaderString(HttpHeaders.CONTENT_TYPE);
            if (contentType != null && contentType.contains(MediaType.TEXT_PLAIN)) {
              data = response.readEntity(String.class);
            } else {
              data = response.readEntity(Object.class);
            }

            Map<String, List<Object>> responseHeaders = new HashMap<>();
            for (String headerName : response.getHeaders().keySet()) {
              responseHeaders.put(headerName, response.getHeaders().get(headerName));
            }

            TunnelingResponse tResponse = new TunnelingResponse();
            tResponse.setId(tRequest.getId());
            tResponse.setStatus(response.getStatus());
            tResponse.setPayload(data);
            tResponse.setHeaders(responseHeaders);

            LOG.debug("Serving: {} {}, status: {}", tRequest.getMethod(), tRequest.getPath(), tResponse.getStatus());

            wsSession.getRemote()
                .sendBytesByFuture(ByteBuffer.wrap(ObjectMapperFactory.get().writeValueAsBytes(tResponse)));
          }
        } finally {
         if (response != null) {
           response.close();
         }
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private Response proxyRequest(TunnelingRequest request) {
    String url = runtimeInfo.getOriginalHttpUrl() + "/";

    if (request.getPath() != null) {
      url += request.getPath();
    }

    if (request.getQueryString() != null) {
      url += "?" + request.getQueryString();
    }

    Invocation.Builder builder = this.httpClient.target(url).request();
    MultivaluedHashMap<String, Object> multivaluedHashMap = new MultivaluedHashMap<>();
    for (Map.Entry<String, List<Object>> entry : request.getHeaders().entrySet()) {
      multivaluedHashMap.addAll(entry.getKey(), entry.getValue());
    }
    builder.headers(multivaluedHashMap);

    if (request.getPayload() != null) {
      return builder.method(request.getMethod(), Entity.entity(request.getPayload(), request.getMediaType()));
    } else {
      return builder.method(request.getMethod());
    }
  }

  private void sendPing() {
    if (this.wsSession != null && this.wsSession.isOpen()) {
      try {
        this.wsSession.getRemote().sendString(PING_MESSAGE);
      } catch (IOException e) {
        LOG.error("Failed to send ping message: {}", e.getMessage(), e);
      }
    }
  }

  public void stopTask() {
    if (this.webSocketClient != null) {
      try {
        this.webSocketClient.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop WebSocket Client: {}", e.getMessage(), e);
      }
    }
  }

  /**
   * Returns true if tunneling application deployed in the Control Hub instance and
   * configuration "dpm.tunneling.enabled" configured to true.
   */
  @VisibleForTesting
  boolean isTunnelingEnabled(String controlHubUrl) {
    boolean enabled = this.conf.get(TUNNELING_ENABLED_CONFIG, TUNNELING_ENABLED_CONFIG_DEFAULT);
    if (enabled) {
      String url = controlHubUrl + AVAILABLE_APPS_ENDPOINT;
      Response response = null;
      try {
        response = ClientBuilder.newClient()
            .target(url)
            .request()
            .header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME)
            .header(SSOConstants.X_APP_AUTH_TOKEN, runtimeInfo.getAppAuthToken())
            .header(SSOConstants.X_APP_COMPONENT_ID, runtimeInfo.getId())
            .get();
        List<String> availableApps = response.readEntity(new GenericType<List<String>>() {});
        enabled = availableApps.contains(TUNNELING_APP_NAME);
      } catch (Exception e) {
        LOG.warn("Exception during fetching all available apps from Control Hub: {}", e.getMessage(), e);
        return false;
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }
    return enabled;
  }
}
