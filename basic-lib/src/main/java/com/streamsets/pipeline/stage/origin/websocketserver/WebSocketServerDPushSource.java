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
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.lib.websocket.WebSocketOriginGroups;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

@StageDef(
    version = 15,
    label = "WebSocket Server",
    description = "Listens for requests on a WebSocket endpoint",
    icon="websockets_multithreaded.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl ="index.html?contextID=task_mzv_cvc_3z",
    sendsResponse = true,
    upgrader = WebSocketServerPushSourceUpgrader.class,
    upgraderDef = "upgrader/WebSocketServerDPushSource.yaml"
)
@ConfigGroups(WebSocketOriginGroups.class)
@HideConfigs(value = {
    "dataFormatConfig.verifyChecksum",
    "dataFormatConfig.avroSchemaSource",
    "webSocketConfigs.tlsConfigBean.useRemoteTrustStore",
    "webSocketConfigs.tlsConfigBean.trustStoreFilePath",
    "webSocketConfigs.tlsConfigBean.trustedCertificates",
    "webSocketConfigs.tlsConfigBean.trustStoreType",
    "webSocketConfigs.tlsConfigBean.trustStorePassword",
    "webSocketConfigs.tlsConfigBean.trustStoreAlgorithm"
})
@GenerateResourceBundle
public class WebSocketServerDPushSource extends DPushSource {

  @ConfigDefBean
  public WebSocketConfigs webSocketConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "HTTP payload data format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDefBean(groups = "WEB_SOCKET_RESPONSE")
  public ResponseConfigBean responseConfig = new ResponseConfigBean();

  @Override
  protected PushSource createPushSource() {
    return new WebSocketServerPushSource(webSocketConfigs, dataFormat, dataFormatConfig, responseConfig);
  }

}
