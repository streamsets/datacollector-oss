/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;

public class S3ErrorConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Error Bucket",
    description = "",
    displayPosition = 10,
    group = "#0"
  )
  public String errorBucket;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Error Folder",
    description = "",
    displayPosition = 20,
    group = "#0"
  )
  public String errorFolder;

}
