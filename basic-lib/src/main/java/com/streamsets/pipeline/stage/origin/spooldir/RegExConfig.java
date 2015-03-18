/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.ConfigDef;

public class RegExConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue="/",
    label = "",
    description = "Hash string fields. You can enter multiple fields for the same hash type.",
    displayPosition = 10
  )
  public String fieldPath;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue="1",
    label = "",
    description="",
    displayPosition = 20
  )
  public int group;

}
