/*
 * ```Copyright 2017 StreamSets Inc.
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
 * limitations under the License.```
 */
package com.streamsets.pipeline.stage.destination.mqtt;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    version = 5,
    label = "Write to MQTT",
    description = "Writes error records to MQTT broker",
    icon = "mqtt.png",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH

    },
    onlineHelpRefUrl ="index.html?contextID=concept_kgc_l4y_5r",
    upgrader = MqttClientTargetUpgrader.class,
    upgraderDef = "upgrader/ToErrorMqttClientDTarget.yaml"
)
@HideConfigs(
    preconditions = true,
    onErrorRecord = true,
    value = {
        "commonConf.tlsConfig.useRemoteKeyStore",
        "commonConf.tlsConfig.keyStoreFilePath",
        "commonConf.tlsConfig.privateKey",
        "commonConf.tlsConfig.certificateChain",
        "commonConf.tlsConfig.keyStoreType",
        "commonConf.tlsConfig.keyStorePassword",
        "commonConf.tlsConfig.keyStoreAlgorithm",
        "publisherConf.dataFormat"
    })
@ErrorStage
@HideStage(HideStage.Type.ERROR_STAGE)
@GenerateResourceBundle
public class ToErrorMqttClientDTarget extends MqttClientDTarget {
  @Override
  protected Target createTarget() {
    publisherConf.dataFormat = DataFormat.SDC_JSON;
    return new MqttClientTarget(commonConf, publisherConf);
  }
}
