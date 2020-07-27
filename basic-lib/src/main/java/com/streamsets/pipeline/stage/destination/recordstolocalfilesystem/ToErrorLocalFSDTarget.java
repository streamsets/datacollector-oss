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
package com.streamsets.pipeline.stage.destination.recordstolocalfilesystem;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.api.el.SdcEL;
import com.streamsets.pipeline.lib.el.TimeEL;

@StageDef(
    version = 1,
    label = "Write to File",
    description = "Writes records to a local File System as SDC records",
    execution = ExecutionMode.STANDALONE,
    upgraderDef = "upgrader/ToErrorLocalFSDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_e33_3v5_1r"
)
@HideConfigs(preconditions = true, onErrorRecord = true)
@ErrorStage
@HideStage(HideStage.Type.ERROR_STAGE)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class ToErrorLocalFSDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Directory",
      description = "Directory to write records",
      displayPosition = 10,
      group = "FILES"
  )
  public String directory;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "sdc-${sdc:id()}",
      label = "Files Prefix",
      description = "File name prefix",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILES",
      elDefs = SdcEL.class
  )
  public String uniquePrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${1 * HOURS}",
      label = "File Wait Time (secs)",
      description = "Max time to wait for error records before creating a new error file. \n" +
                    "Enter the time in seconds or use the default expression to enter the time limit in minutes. " +
                    "You can also use HOURS in the expression to enter the limit in hours.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES",
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String rotationIntervalSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "512",
      label = "Max File Size (MB)",
      description = "Max file size to trigger the creation of a new file. Use 0 to opt out.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int maxFileSizeMbs;


  @Override
  protected Target createTarget() {
    return new RecordsToLocalFileSystemTarget(directory, uniquePrefix, rotationIntervalSecs, maxFileSizeMbs);
  }

}
