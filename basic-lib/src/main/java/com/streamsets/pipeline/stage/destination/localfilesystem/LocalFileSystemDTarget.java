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
package com.streamsets.pipeline.stage.destination.localfilesystem;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTarget;

@StageDef(
    version = 4,
    label = "Local FS",
    description = "Writes to the local file system",
    icon = "localfilesystem.png",
    producesEvents = true,
    eventDefs = {WholeFileProcessedEvent.class},
    upgrader = LocalFileSystemTargetUpgrader.class,
    upgraderDef = "upgrader/LocalFileSystemDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_e33_3v5_1r"
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {
    "configs.hdfsUri",
    "configs.hdfsUser",
    "configs.hdfsKerberos",
    "configs.hdfsConfDir",
    "configs.hdfsConfigs",
    "configs.seqFileCompressionType",
    "configs.dataGeneratorFormatConfig.includeSchema",
})
@GenerateResourceBundle
public class LocalFileSystemDTarget extends DTarget {

  @ConfigDefBean
  public LocalFileSystemConfigBean configs;

  @Override
  protected Target createTarget() {
    return new HdfsTarget(configs);
  }

}
