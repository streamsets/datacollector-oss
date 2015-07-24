/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmerger;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;

public class FieldMergerConfig {

  /**
   * Parameter-less constructor required.
   */
  public FieldMergerConfig() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "From Field",
      description = "The field in the incoming record to merge.",
      displayPosition = 10
  )
  @FieldSelector(singleValued = true)
  public String fromField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "To Field",
      description="The field to merge into.",
      displayPosition = 20
  )
  public String toField;
}
