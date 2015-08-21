/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.stage.lib.kinesis.AWSRegionChooserValues;

public class S3Config {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "US_WEST_2",
    label = "Region",
    displayPosition = 10,
    group = "#0"
  )
  @ValueChooser(AWSRegionChooserValues.class)
  public Regions region;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Access Key Id",
    description = "",
    displayPosition = 20,
    group = "#0"
  )
  public String accessKeyId;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Secret Access Key",
    description = "",
    displayPosition = 30,
    group = "#0"
  )
  public String secretAccessKey;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Bucket",
    description = "",
    displayPosition = 40,
    group = "#0"
  )
  public String bucket;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Folder",
    description = "",
    displayPosition = 50,
    group = "#0"
  )
  public String folder;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Object Name Prefix",
    description = "",
    displayPosition = 60,
    group = "#0"
  )
  public String prefix;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Object Path delimiter",
    description = "",
    defaultValue = "/",
    displayPosition = 70,
    group = "#0"
  )
  public String delimiter;
}
