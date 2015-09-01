/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.PostProcessingOptions;

public class S3ErrorConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "NONE",
    label = "Error Handling Option",
    description = "Action to take when an error is encountered",
    displayPosition = 10,
    group = "#0"
  )
  @ValueChooserModel(S3PostProcessingChooserValues.class)
  public PostProcessingOptions errorHandlingOption;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "MOVE_TO_DIRECTORY",
    label = "Archiving Option",
    displayPosition = 20,
    group = "#0",
    dependsOn = "errorHandlingOption",
    triggeredByValue = { "ARCHIVE" }
  )
  @ValueChooserModel(S3ArchivingOptionChooserValues.class)
  public S3ArchivingOption archivingOption;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Error Folder",
    description = "Files in error will be moved into this folder",
    displayPosition = 30,
    group = "#0",
    dependsOn = "errorHandlingOption",
    triggeredByValue = { "ARCHIVE" }
  )
  public String errorFolder;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Error Bucket",
    description = "Files in error will be moved into this bucket",
    displayPosition = 40,
    group = "#0",
    dependsOn = "archivingOption",
    triggeredByValue = { "MOVE_TO_BUCKET" }
  )
  public String errorBucket;

}
