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
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@StageDef(
  // We're reusing upgrader for both ToErrorSdcIpcDTarget and SdcIpcDTarget, make sure that you
  // upgrade both versions at the same time when changing.
    version = 3,
    label = "SDC RPC",
    description = "Sends records via SDC RPC to a Data Collector pipeline that uses an SDC RPC origin",
    icon="sdcipc.png",
    onlineHelpRefUrl ="index.html?contextID=task_nbl_r2x_dt",
    upgrader = SdcIpcTargetUpgrader.class,
    upgraderDef = "upgrader/SdcIpcDTarget.yaml"
)
@ConfigGroups(Groups.class)
@HideConfigs({
    "config.tlsConfigBean.useRemoteKeyStore",
    "config.tlsConfigBean.keyStoreFilePath",
    "config.tlsConfigBean.privateKey",
    "config.tlsConfigBean.certificateChain",
    "config.tlsConfigBean.keyStoreType",
    "config.tlsConfigBean.keyStorePassword",
    "config.tlsConfigBean.keyStoreAlgorithm"
})
@GenerateResourceBundle
public class SdcIpcDTarget extends DTarget {

  @ConfigDefBean
  public Configs config;

  @Override
  protected Target createTarget() {
    return new SdcIpcTarget(config);
  }
}
