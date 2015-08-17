/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ConfigDef;

public class BasicConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Max Batch Size (records)",
    description = "Max number of records per batch",
    displayPosition = 81,
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
    displayPosition = 82,
    group = "#0",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int maxWaitTime = 2000;
}
