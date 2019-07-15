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
package com.streamsets.pipeline.stage.destination.coap;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.coap.Groups;

@StageDef(
    version = 1,
    label = "CoAP Client",
    description = "Uses a CoAP client to write data",
    icon = "coap.png",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH
    },
    recordsByRef = true,
    upgraderDef = "upgrader/CoapClientDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_d2p_w3n_sz"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class CoapClientDTarget extends DTarget {

  @ConfigDefBean
  public CoapClientTargetConfig conf;

  @Override
  protected Target createTarget() {
    return new CoapClientTarget(conf);
  }
}
