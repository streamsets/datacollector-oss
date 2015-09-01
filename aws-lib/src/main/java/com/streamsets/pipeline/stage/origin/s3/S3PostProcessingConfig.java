/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.PostProcessingOptions;

public class S3PostProcessingConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "NONE",
    label = "Post Processing Option",
    description = "Action to take after processing an object",
    displayPosition = 10,
    group = "#0"
  )
  @ValueChooserModel(S3PostProcessingChooserValues.class)
  public PostProcessingOptions postProcessing;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "MOVE_TO_DIRECTORY",
    label = "Archiving Option",
    displayPosition = 20,
    group = "#0",
    dependsOn = "postProcessing",
    triggeredByValue = { "ARCHIVE" }
  )
  @ValueChooserModel(S3ArchivingOptionChooserValues.class)
  public S3ArchivingOption archivingOption;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Post Process Folder",
    description = "",
    displayPosition = 30,
    group = "#0",
    dependsOn = "postProcessing",
    triggeredByValue = { "ARCHIVE" }
  )
  public String postProcessFolder;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Post Process Bucket",
    description = "",
    displayPosition = 40,
    group = "#0",
    dependsOn = "archivingOption",
    triggeredByValue = { "MOVE_TO_BUCKET" }
  )
  public String postProcessBucket;

}
