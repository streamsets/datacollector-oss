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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.destination.lib.ToOriginResponseConfig;

@StageDef(
    // We're reusing upgrader for both ToErrorKinesisDTarget & KinesisDTarget, make sure that you
    // upgrade both versions at the same time when changing.
    version = 10,
    label = "Kinesis Producer",
    description = "Writes data to Amazon Kinesis",
    icon = "kinesis.png",
    upgrader = KinesisTargetUpgrader.class,
    upgraderDef = "upgrader/KinesisDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_q2j_ml4_yr",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH
    }
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@HideConfigs(value = {
    "kinesisConfig.connection.proxyConfig.connectionTimeout",
    "kinesisConfig.connection.proxyConfig.socketTimeout",
    "kinesisConfig.connection.proxyConfig.retryCount",
    "kinesisConfig.connection.proxyConfig.useProxy",
})
public class KinesisDTarget extends DTarget {

  @ConfigDefBean(groups = {"KINESIS", "DATA_FORMAT"})
  public KinesisProducerConfigBean kinesisConfig;

  @ConfigDefBean(groups = {"RESPONSE"})
  public ToOriginResponseConfig responseConf = new ToOriginResponseConfig();

  @Override
  protected Target createTarget() {
    return new KinesisTarget(kinesisConfig, responseConf);
  }
}
