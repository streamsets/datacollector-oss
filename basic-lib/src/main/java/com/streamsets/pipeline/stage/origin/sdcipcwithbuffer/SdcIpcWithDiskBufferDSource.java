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
package com.streamsets.pipeline.stage.origin.sdcipcwithbuffer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.origin.sdcipc.Configs;

@StageDef(
    version = 4,
    label = "Dev SDC RPC with Buffering",
    description = "Receives records via SDC RPC from a Data Collector pipeline that uses an SDC RPC destination. " +
        "It buffers records in memory/disk. In case of failure/stop records may be lost.",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    icon = "dev.png",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Pipeline_Design/DevStages.html",
    upgrader = SdcIpcWithDiskBufferSourceUpgrader.class,
    upgraderDef = "upgrader/SdcIpcWithDiskBufferDSource.yaml"
)
@ConfigGroups(Groups.class)
@HideConfigs({
    "configs.tlsConfigBean.useRemoteTrustStore",
    "configs.tlsConfigBean.trustStoreFilePath",
    "configs.tlsConfigBean.trustedCertificates",
    "configs.tlsConfigBean.trustStoreType",
    "configs.tlsConfigBean.trustStorePassword",
    "configs.tlsConfigBean.trustStoreAlgorithm"
})
@GenerateResourceBundle
public class SdcIpcWithDiskBufferDSource extends DSourceOffsetCommitter {

  @ConfigDefBean
  public Configs configs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "900",
      label = "Max Fragments in Memory",
      displayPosition = 100,
      group = "ADVANCED",
      min = 1,
      max = 10000)
  public int maxFragmentsInMemory;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "100",
      label = "Max Disk Buffer (MB)",
      displayPosition = 110,
      group = "ADVANCED",
      min = 1,
      max = 10000)
  public int maxDiskBufferMB;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Wait Time for Empty Batches (millisecs)",
      displayPosition = 120,
      group = "ADVANCED",
      min = 1,
      max = 10000)
  public long waitTimeForEmptyBatches;

  @Override
  protected Source createSource() {
    return new SdcIpcWithDiskBufferSource(configs, maxFragmentsInMemory, maxDiskBufferMB, waitTimeForEmptyBatches);
  }
}
