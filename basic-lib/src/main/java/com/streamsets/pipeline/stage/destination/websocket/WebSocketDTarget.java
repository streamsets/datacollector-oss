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
package com.streamsets.pipeline.stage.destination.websocket;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.websocket.Groups;

@StageDef(
    version = 3,
    label = "WebSocket Client",
    description = "Uses a WebSocket client to write data",
    icon = "websockets.png",
    recordsByRef = true,
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH

    },
    onlineHelpRefUrl ="index.html?contextID=task_erb_pjn_lz",
    upgrader = WebSocketTargetUpgrader.class,
    upgraderDef = "upgrader/WebSocketDTarget.yaml"
)
@ConfigGroups(Groups.class)
@HideConfigs({
    "conf.tlsConfig.useRemoteKeyStore",
    "conf.tlsConfig.keyStoreFilePath",
    "conf.tlsConfig.privateKey",
    "conf.tlsConfig.certificateChain",
    "conf.tlsConfig.keyStoreType",
    "conf.tlsConfig.keyStorePassword",
    "conf.tlsConfig.keyStoreAlgorithm"
})
@GenerateResourceBundle
public class WebSocketDTarget extends DTarget {

  @ConfigDefBean
  public WebSocketTargetConfig conf;

  @Override
  protected Target createTarget() {
    return new WebSocketTarget(conf);
  }
}
