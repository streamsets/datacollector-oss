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
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSourceOffsetCommitter;

@StageDef(
    version = 2,
    label = "SDC RPC",
    execution = ExecutionMode.STANDALONE,
    description = "Receives records via SDC RPC from a Data Collector pipeline that uses an SDC RPC destination",
    icon="sdcipc.png",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Origins/SDC_RPCorigin.html#task_lxh_1w2_ct",
    upgrader = SdcIpcSourceUpgrader.class
)
@ConfigGroups(Groups.class)
@HideConfigs({
    "configs.tlsConfigBean.trustStoreFilePath",
    "configs.tlsConfigBean.trustStoreType",
    "configs.tlsConfigBean.trustStorePassword",
    "configs.tlsConfigBean.trustStoreAlgorithm"
})
@GenerateResourceBundle
public class SdcIpcDSource extends DSourceOffsetCommitter {

  @ConfigDefBean
  public Configs configs;

  @Override
  protected Source createSource() {
    return new SdcIpcSource(configs);
  }
}
