/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=1,
    label="Expression Evaluator",
    description="Performs calculations on a field-by-field basis",
    icon="expression.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class ExpressionDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Expressions",
      description = "",
      displayPosition = 10,
      group = "EXPRESSIONS"
  )
  @ComplexField
  public List<ExpressionProcessorConfig> expressionProcessorConfigs;

  @Override
  protected Processor createProcessor() {
    return new ExpressionProcessor(expressionProcessorConfigs);
  }

}
