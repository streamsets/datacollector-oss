/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.AbstractHttpReceiverServer;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.httpsource.AbstractHttpServerPushSource;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RestServicePushSource extends AbstractHttpServerPushSource<HttpReceiver> {

  public static final String REST_SERVICE_METRICS = "restService";
  public static final String API_ENDPOINT = "apiEndpoint";
  private final HttpSourceConfigs httpConfigs;
  private final DataFormat dataFormat;
  private final DataParserFormatConfig dataFormatConfig;
  private final ResponseConfigBean responseConfig;
  private RestServiceGatewayInfo gatewayInfo;
  private Map<String, Object> gaugeMap;

  RestServicePushSource(
      HttpSourceConfigs httpConfigs,
      int maxRequestSizeMB,
      DataFormat dataFormat,
      DataParserFormatConfig dataFormatConfig,
      ResponseConfigBean responseConfig
  ) {
    super(httpConfigs, new RestServiceReceiver(
        httpConfigs,
        maxRequestSizeMB,
        dataFormatConfig,
        responseConfig
    ));
    this.httpConfigs = httpConfigs;
    this.dataFormat = dataFormat;
    this.dataFormatConfig = dataFormatConfig;
    this.responseConfig = responseConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = getHttpConfigs().init(getContext());
    dataFormatConfig.stringBuilderPoolSize = httpConfigs.getMaxConcurrentRequests();
    dataFormatConfig.init(
        getContext(),
        dataFormat,
        Groups.DATA_FORMAT.name(),
        "dataFormatConfig",
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        issues
    );

    responseConfig.dataGeneratorFormatConfig.init(
        getContext(),
        responseConfig.dataFormat,
        Groups.HTTP_RESPONSE.name(),
        "responseConfig.dataGeneratorFormatConfig",
        issues
    );

    issues.addAll(getReceiver().init(getContext()));
    if (issues.isEmpty()) {
      issues.addAll(super.init());
    }

    if (httpConfigs.useApiGateway()) {
      String secret = UUID.randomUUID().toString();
      httpConfigs.setGatewaySecret(secret);
      gatewayInfo = new RestServiceGatewayInfo(
          getContext().getPipelineId(),
          httpConfigs.getGatewayServiceName(),
          httpConfigs.getNeedGatewayAuth(),
          secret
      );
    }
    this.gaugeMap = getContext().createGauge(REST_SERVICE_METRICS).getValue();

    return issues;
  }

  protected AbstractHttpReceiverServer getHttpReceiver() {
    return  new RestServiceReceiverServer(httpConfigs, getReceiver(), getErrorQueue());
  }

  protected void onServerStart(String serverUrl) {
    if (gatewayInfo != null) {
      gatewayInfo.setServiceUrl(serverUrl);
      String gatewayEndpoint = getContext().registerApiGateway(gatewayInfo);
      gaugeMap.put(API_ENDPOINT, gatewayEndpoint);
    } else {
      gaugeMap.put(API_ENDPOINT, serverUrl);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    if (gatewayInfo != null) {
      getContext().unregisterApiGateway(gatewayInfo);
    }
    getReceiver().destroy();
    httpConfigs.destroy();
  }

}
