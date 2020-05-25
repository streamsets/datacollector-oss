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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StatsAggregatorStage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.Arrays;

@StageDef(
    version = 3,
    label = "Write to SDC RPC",
    description = "Writes pipeline Statistic records to another pipeline over SDC RPC",
    icon="sdcipc.png",
    onlineHelpRefUrl = "",
    upgrader = StatsSdcIpcTargetUpgrader.class,
    upgraderDef = "upgrader/StatsSdcIpcDTarget.yaml"
)
@ConfigGroups(Groups.class)
@StatsAggregatorStage
@HideStage(HideStage.Type.STATS_AGGREGATOR_STAGE)
@HideConfigs(
    preconditions = true,
    onErrorRecord = true,
    value = {"config.hostPorts", "config.appId"}
)
@GenerateResourceBundle
public class StatsSdcIpcDTarget extends DTarget {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "SDC RPC Connection",
    description = "System pipeline will be started on the specified host and port. Use the format <host>:<port>.",
    displayPosition = 10,
    group = "RPC"
  )
  public String hostPorts;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.CREDENTIAL,
    label = "SDC RPC ID",
    description = "The id to be assigned to the system pipeline.",
    displayPosition = 20,
    group = "RPC"
  )
  public CredentialValue appId;

  @ConfigDefBean
  public Configs config;

  @Override
  protected Target createTarget() {
    config.retryDuringValidation = true;
    config.hostPorts = Arrays.asList(hostPorts);
    config.appId = appId;
    return new SdcIpcTarget(config);
  }
}
