/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;

import java.util.List;

public class S3FileConfig {

  private static final int MIN_OVERRUN_LIMIT = 64 * 1000;
  private static final int MAX_OVERRUN_LIMIT = 1000 * 1000;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "Buffer Limit (KB)",
    defaultValue = "64000",
    description = "Low level reader buffer limit to avoid out of Memory errors",
    displayPosition = 100,
    group = "#0",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int overrunLimit;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "10",
    label = "Max Objects in Directory",
    description = "Max number of files in the directory waiting to be processed. Additional files cause the " +
      "pipeline to fail.",
    displayPosition = 110,
    group = "#0",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int maxSpoolObjects;

  public boolean validate(Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean valid = true;
    if (overrunLimit < MIN_OVERRUN_LIMIT || overrunLimit >= MAX_OVERRUN_LIMIT) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "overrunLimit", Errors.S3_SPOOLDIR_06));
      valid = false;
    }

    if (maxSpoolObjects < 1) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "maxSpoolObjects", Errors.S3_SPOOLDIR_13));
      valid = false;
    }

    return valid;
  }
}
