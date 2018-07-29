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

import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.lib.websocket.Groups;
import com.streamsets.pipeline.lib.websocket.WebSocketOriginGroups;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;

public class WebSocketServerPushSource extends AbstractWebSocketServerPushSource<WebSocketReceiver> {

  private final WebSocketConfigs webSocketConfigs;
  private final DataFormat dataFormat;
  private final DataParserFormatConfig dataFormatConfig;
  private final ResponseConfigBean responseConfig;

  public WebSocketServerPushSource(
      WebSocketConfigs webSocketConfigs,
      DataFormat dataFormat,
      DataParserFormatConfig dataFormatConfig,
      ResponseConfigBean responseConfig
  ) {
    super(webSocketConfigs, new PushWebSocketReceiver(webSocketConfigs, dataFormatConfig, responseConfig));
    this.webSocketConfigs = webSocketConfigs;
    this.dataFormat = dataFormat;
    this.dataFormatConfig = dataFormatConfig;
    this.responseConfig = responseConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = webSocketConfigs.init(getContext());
    dataFormatConfig.stringBuilderPoolSize = webSocketConfigs.getMaxConcurrentRequests();
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
        WebSocketOriginGroups.WEB_SOCKET_RESPONSE.name(),
        "responseConfig.dataGeneratorFormatConfig",
        issues
    );
    issues.addAll(getReceiver().init(getContext()));
    if (issues.isEmpty()) {
      issues.addAll(super.init());
    }
    return issues;
  }

  @Override
  public void destroy() {
    super.destroy();
    getReceiver().destroy();
    webSocketConfigs.destroy();
  }
}
