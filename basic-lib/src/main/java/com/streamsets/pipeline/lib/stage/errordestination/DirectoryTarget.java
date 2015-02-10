/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.errordestination;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Local FileSystem",
    description = "Writes Data Collector records to the local FileSystem",
    icon="localfilesystem.png",
    requiredFields = false
)
@ErrorStage
@ConfigGroups(DirectoryTarget.Groups.class)
public class DirectoryTarget extends BaseTarget {

  public enum Groups implements Label {
    FILES;

    @Override
    public String getLabel() {
      return "Files";
    }
  }
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Directory",
      description = "Directory to write bad records",
      displayPosition = 10,
      group = "FILES"
  )
  public String directory;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_NUMBER,
      defaultValue = "${1 * HOUR}",
      label = "Rotation Interval",
      description = "Bad records file rotation interval",
      displayPosition = 20,
      group = "FILES"
  )
  public long rotationIntervalSecs;

  @Override
  public void write(Batch batch) throws StageException {
  }

}
