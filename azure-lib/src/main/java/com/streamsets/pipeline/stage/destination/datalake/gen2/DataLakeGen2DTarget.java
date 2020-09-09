/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.datalake.gen2;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@StageDef(
    version = 2,
    label = "Azure Data Lake Storage Gen2",
    description = "Writes data to Azure Data Lake Storage Gen2",
    icon = "data-lake-store-gen2.png",
    producesEvents = true,
    upgrader = DataLakeGen2TargetUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_vk2_j45_rhb"
)
@ConfigGroups(DataLakeGen2TargetGroups.class)
@HideConfigs(value = {
    "dataLakeGen2TargetConfig.hdfsUri",
    "dataLakeGen2TargetConfig.hdfsUser",
    "dataLakeGen2TargetConfig.hdfsKerberos",
    "dataLakeGen2TargetConfig.hdfsConfDir",
    "dataLakeGen2TargetConfig.hdfsConfigs",
    "dataLakeGen2TargetConfig.hdfsConfigs"
})
@GenerateResourceBundle
public class DataLakeGen2DTarget extends DTarget {

  @ConfigDefBean
  public DataLakeGen2TargetConfigBean dataLakeGen2TargetConfig;

  @Override
  protected Target createTarget() {
    return new DataLakeGen2Target(dataLakeGen2TargetConfig);
  }
}
