/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;

import java.util.List;

public class BasicConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Max Batch Size (records)",
    description = "Max number of records per batch",
    displayPosition = 1000,
    group = "#0",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int maxBatchSize = 1000;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "2000",
    label = "Batch Wait Time (ms)",
    description = "Max time to wait for data before sending a partial or empty batch",
    displayPosition = 1010,
    group = "#0",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int maxWaitTime = 2000;

  public boolean validate(List<Stage.ConfigIssue> issues, String group, Stage.Context context) {
    boolean valid = true;

    if (maxBatchSize < 1) {
      issues.add(context.createConfigIssue(group, "maxBatchSize", BasicErrors.BASIC_01, maxBatchSize));
      valid = false;
    }

    if (maxWaitTime < 1) {
      issues.add(context.createConfigIssue(group, "maxWaitTime", BasicErrors.BASIC_02, maxWaitTime));
      valid = false;
    }

    return valid;
  }
}
