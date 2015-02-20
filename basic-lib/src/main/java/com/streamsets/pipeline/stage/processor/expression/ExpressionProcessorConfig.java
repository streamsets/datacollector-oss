/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;

public class ExpressionProcessorConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/",
      label = "Output Field",
      description = "Use an existing field or enter a new field. Using an existing field overwrites the " +
                    "original value.",
      displayPosition = 10
  )
  @FieldSelector(singleValued = true)
  public String fieldToSet;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_OBJECT,
      defaultValue = "${record:value('/')}",
      label = "Expression",
      description = "Use the expression language to modify values in a field.",
      displayPosition = 20
  )
  public String expression;

}
