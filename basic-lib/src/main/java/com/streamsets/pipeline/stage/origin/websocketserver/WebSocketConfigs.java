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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.lib.websocket.Groups;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WebSocketConfigs extends HttpConfigs {

  public WebSocketConfigs() {
    super(Groups.WEB_SOCKET.name(), "webSocketConfigs.");
  }

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "8080",
      label = "WebSocket Listening Port",
      description = "WebSocket endpoint to listen for data.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "WEB_SOCKET",
      min = 1,
      max = 65535
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Max Concurrent Requests",
      description = "Maximum number of concurrent requests allowed by the origin.",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "WEB_SOCKET",
      min = 1,
      max = 200
  )
  public int maxConcurrentRequests;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Application ID",
      description = "Only WebSocket requests presenting this token will be accepted.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "WEB_SOCKET"
  )
  public CredentialValue appId = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Application ID in URL",
      description = "Use when the application ID is included in a query parameter in the URL instead of in the request header - ws://localhost:8000?sdcApplicationId=<Application ID>",
      displayPosition = 21,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "WEB_SOCKET"
  )
  public boolean appIdViaQueryParamAllowed;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Request Size (MB)",
      defaultValue = "100",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "WEB_SOCKET",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxRequestSizeMB;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Idle Timeout (ms)",
      defaultValue = "20000",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "WEB_SOCKET",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int idleTimeout;

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getMaxConcurrentRequests() {
    return maxConcurrentRequests;
  }

  @Override
  public List<CredentialValue> getAppIds() {
    return new ArrayList<>(Arrays.asList(appId));
  }

  @Override
  public int getMaxHttpRequestSizeKB() {
    return maxRequestSizeMB;
  }

  @Override
  public boolean isTlsEnabled() {
    return tlsConfigBean.tlsEnabled;
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return appIdViaQueryParamAllowed;
  }

  @Override
  public TlsConfigBean getTlsConfigBean() {
    return tlsConfigBean;
  }

  @Override
  public boolean isApplicationIdEnabled() {
    return true;
  }

  int getMaxRequestSizeMB() {
    return maxRequestSizeMB;
  }

  int getIdleTimeout() {
    return idleTimeout;
  }
}
