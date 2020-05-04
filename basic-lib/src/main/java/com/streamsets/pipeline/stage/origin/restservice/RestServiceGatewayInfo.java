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
package com.streamsets.pipeline.stage.origin.restservice;

import com.streamsets.pipeline.api.gateway.GatewayInfo;

public class RestServiceGatewayInfo implements GatewayInfo {

  private final String pipelineId;
  private final String serviceName;
  private final boolean needGatewayAuth;
  private final String secret;
  private String serviceUrl;

  public RestServiceGatewayInfo(
      String pipelineId,
      String serviceName,
      boolean needGatewayAuth,
      String secret
  ) {
    this.pipelineId = pipelineId;
    this.serviceName = serviceName;
    this.needGatewayAuth = needGatewayAuth;
    this.secret = secret;
  }

  @Override
  public String getPipelineId() {
    return pipelineId;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }

  @Override
  public String getServiceUrl() {
    return serviceUrl;
  }

  @Override
  public boolean getNeedGatewayAuth() {
    return needGatewayAuth;
  }

  @Override
  public String getSecret() {
    return secret;
  }

  public void setServiceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
  }
}
