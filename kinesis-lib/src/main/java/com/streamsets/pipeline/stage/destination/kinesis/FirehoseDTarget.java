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

@StageDef(
    version = 6,
    label = "Kinesis Firehose",
    description = "Writes data to Amazon Kinesis Firehose",
    icon = "kinesisfirehose.png",
    upgrader = FirehoseTargetUpgrader.class,
    upgraderDef = "upgrader/FirehoseDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_rpf_qbq_kv",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH
    }
)
@ConfigGroups(value = FirehoseGroups.class)
@GenerateResourceBundle
@HideConfigs(value = {
    "kinesisConfig.connection.proxyConfig.connectionTimeout",
    "kinesisConfig.connection.proxyConfig.socketTimeout",
    "kinesisConfig.connection.proxyConfig.retryCount",
    "kinesisConfig.connection.proxyConfig.useProxy",
})
public class FirehoseDTarget extends DTarget {

  @ConfigDefBean(groups = {"KINESIS", "DATA_FORMAT"})
  public FirehoseConfigBean kinesisConfig;

  @Override
  protected Target createTarget() {
    return new FirehoseTarget(kinesisConfig);
  }
}
