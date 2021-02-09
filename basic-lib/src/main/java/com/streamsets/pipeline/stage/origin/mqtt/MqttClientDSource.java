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
package com.streamsets.pipeline.stage.origin.mqtt;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.mqtt.Groups;
import com.streamsets.pipeline.lib.mqtt.MqttClientConfigBean;

@StageDef(
    version = 6,
    label = "MQTT Subscriber",
    description = "Uses an MQTT client to subscribe to a topic on the MQTT Broker",
    icon = "mqtt_multithreaded.png",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    recordsByRef = true,
    onlineHelpRefUrl ="index.html?contextID=task_wnz_mxt_lz",
    upgrader = MqttClientSourceUpgrader.class,
    upgraderDef = "upgrader/MqttClientDSource.yaml"
)
@HideConfigs({
    "subscriberConf.dataFormatConfig.jsonContent"
})
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class MqttClientDSource extends DPushSource {

  @ConfigDefBean
  public MqttClientConfigBean commonConf;

  @ConfigDefBean
  public MqttClientSourceConfigBean subscriberConf;


  @Override
  protected PushSource createPushSource() {
    return new MqttClientSource(commonConf, subscriberConf);
  }
}
