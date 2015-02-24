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
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Expression Evaluator",
    description="Performs calculations on a field-by-field basis",
    icon="expression.png"
)
@ConfigGroups(Groups.class)
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

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MAP,
      label = "Constants",
      description = "Can be used in any expression in the processor.",
      displayPosition = 20,
      group = "EXPRESSIONS"
  )
  public Map<String, ?> constants;

  @Override
  protected Processor createProcessor() {
    return new ExpressionProcessor(expressionProcessorConfigs, constants);
  }

}
