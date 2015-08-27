/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;

public class S3ErrorConfig {

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Error Bucket",
    description = "Move objects in error into this bucket if specified. " +
      "One of 2 configurations, error bucket or error folder, must be specified to move objects in error",
    displayPosition = 10,
    group = "#0"
  )
  public String errorBucket;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Error Folder",
    description = "Move objects in error into this folder if specified. " +
      "One of 2 configurations, error bucket or error folder, must be specified to move objects in error",
    displayPosition = 20,
    group = "#0"
  )
  public String errorFolder;

}
