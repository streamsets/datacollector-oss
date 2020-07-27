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
package com.streamsets.pipeline.lib.httpsource;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

public abstract class CommonHttpConfigs extends HttpConfigs {

  public CommonHttpConfigs() {
    super("HTTP", "httpConfigs.");
  }

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Require Client Authentication",
      description = "Enable if SSL needs client authentication (MASSL/MTLS/Two-Way-SSL/Mutual SSL authentication " +
          "Support). Any client that has its certificate in the server's truststore will be able to establish a TLS " +
          "connection to the server",
      defaultValue = "false",
      displayPosition = 1010,
      group = "TLS",
      dependsOn = "tlsConfigBean.tlsEnabled",
      triggeredByValue = "true"
  )
  public boolean needClientAuth = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use API Gateway",
      description = "Enable to use Data Collector instance as API gateway",
      defaultValue = "false",
      displayPosition = 5,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  public boolean useApiGateway = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Gateway Service Name",
      description = "Name of the Gateway Service used in the REST Service URL - " +
          "<Data Collector Instance URL>/rest/v1/gateway/<service name>",
      displayPosition = 7,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP",
      dependsOn = "useApiGateway",
      triggeredByValue = "true"
  )
  public String serviceName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Require Gateway Authentication",
      description = "Enable to use protected Data Collector instance URL " +
          "<Data Collector Instance URL>/rest/v1/gateway/<service name>. If not enabled, " +
          "REST service uses unprotected Data Collector instance URL " +
          "<Data Collector Instance URL>/public-rest/v1/gateway/<service name>",
      defaultValue = "false",
      displayPosition = 8,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP",
      dependsOn = "useApiGateway",
      triggeredByValue = "true"
  )
  public boolean needGatewayAuth = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "8000",
      label = "HTTP Listening Port",
      description = "HTTP endpoint to listen for data",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP",
      min = 1,
      max = 65535/**,
      dependsOn = "useApiGateway",
      triggeredByValue = "false"*/
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Max Concurrent Requests",
      description = "Maximum number of concurrent requests allowed by the origin",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      min = 1,
      max = 200
  )
  public int maxConcurrentRequests;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Application ID in URL",
      description = "Select if the application ID is in a URL query parameter rather than in the request header " +
          "- http://localhost:8000?sdcApplicationId=<Application ID>",
      displayPosition = 21,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public boolean appIdViaQueryParamAllowed;

  @Override
  public int getPort() {
    if (useApiGateway) {
      return 0;
    }
    return port;
  }

  @Override
  public int getMaxConcurrentRequests() {
    return maxConcurrentRequests;
  }

  private int maxHttpRequestSizeKB = -1;

  // in MBs
  public void setMaxHttpRequestSizeKB(int size) {
    maxHttpRequestSizeKB = size;
  }

  @Override
  public int getMaxHttpRequestSizeKB() {
    return maxHttpRequestSizeKB;
  }

  @Override
  public boolean isTlsEnabled() {
    return tlsConfigBean.isEnabled();
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
  public boolean getNeedClientAuth() {
    return needClientAuth;
  }

  @Override
  public boolean useApiGateway() {
    return useApiGateway;
  }

  @Override
  public String getGatewayServiceName() {
    return serviceName;
  }

  @Override
  public boolean getNeedGatewayAuth() {
    return needGatewayAuth;
  }

  private String gatewaySecret = "";

  public void setGatewaySecret(String secret) {
    gatewaySecret = secret;
  }

  @Override
  public String getGatewaySecret() {
    return gatewaySecret;
  }
}

