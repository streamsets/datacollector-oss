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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;

@StageDef(
    version = 4,
    label = "OPC UA Client",
    description = "Uses an OPC UA Client to read data from an OPC UA Server.",
    icon = "opcua.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl ="index.html?contextID=task_bqt_mx3_h1b",
    upgrader = OpcUaClientSourceUpgrader.class,
    upgraderDef = "upgrader/OpcUaClientDSource.yaml"
)
@ConfigGroups(Groups.class)
@HideConfigs({
    "conf.tlsConfig.useRemoteTrustStore",
    "conf.tlsConfig.trustStoreFilePath",
    "conf.tlsConfig.trustedCertificates",
    "conf.tlsConfig.trustStoreType",
    "conf.tlsConfig.trustStorePassword",
    "conf.tlsConfig.trustStoreAlgorithm"
})
@GenerateResourceBundle
public class OpcUaClientDSource extends DPushSource {

  @ConfigDefBean
  public OpcUaClientSourceConfigBean conf;

  @Override
  protected PushSource createPushSource() {
    return new OpcUaClientSource(conf);
  }
}
