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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;

@StageDef(
    version = 5,
    label = "Hadoop FS",
    description = "Writes to a Hadoop file system",
    icon = "hdfs.png",
    privateClassLoader = true,
    upgrader = HdfsTargetUpgrader.class,
    upgraderDef = "upgrader/HdfsDTarget.yaml",
    producesEvents = true,
    eventDefs = {WholeFileProcessedEvent.class},
    onlineHelpRefUrl ="index.html?contextID=task_m2m_skm_zq"
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {"hdfsTargetConfigBean.dataGeneratorFormatConfig.includeSchema"})
@GenerateResourceBundle
public class HdfsDTarget extends DTarget {

  @ConfigDefBean
  public HdfsTargetConfigBean hdfsTargetConfigBean;

  @Override
  protected Target createTarget() {
    return new HdfsTarget(hdfsTargetConfigBean);
  }

}
