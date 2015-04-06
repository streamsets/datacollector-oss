/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.recordstolocalfilesystem;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.TimeEL;

@StageDef(
    version = "1.0.0",
    label = "SDC Record Files",
    description = "Writes records to the local File System using 'SDC Record (JSON)' format",
    icon="localfilesystem.png"
)
@HideConfig(requiredFields = true, onErrorRecord = true)
@ErrorStage(label = "Write to File")
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class RecordsToLocalFileSystemDTarget extends DTarget {

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
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${1 * HOURS}",
      label = "File Wait Time (secs)",
      description = "Max time to wait for error records before creating a new error file. \n" +
                    "Enter the time in seconds or use the default expression to enter the time limit in minutes. " +
                    "You can also use HOURS in the expression to enter the limit in hours.",
      displayPosition = 20,
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
      displayPosition = 30,
      group = "FILES",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int maxFileSizeMbs;


  @Override
  protected Target createTarget() {
    return new RecordsToLocalFileSystemTarget(directory, rotationIntervalSecs, maxFileSizeMbs);
  }

}
