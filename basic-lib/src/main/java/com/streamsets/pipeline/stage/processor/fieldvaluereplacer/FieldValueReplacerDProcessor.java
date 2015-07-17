/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=1,
    label="Value Replacer",
    description = "Replaces null values with a constant and replaces values with NULL",
    icon="replacer.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldValueReplacerDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields to Null",
      description="Replaces existing values with null values",
      displayPosition = 10,
      group = "REPLACE"
  )
  @FieldSelector
  public List<String> fieldsToNull;

  @ConfigDef(
      required = false,
      type = Type.MODEL, defaultValue="",
      label = "Replace Null Values",
      description="Replaces the null values in a field with a specified value.",
      displayPosition = 20,
      group = "REPLACE"
  )
  @ComplexField(FieldValueReplacerConfig.class)
  public List<FieldValueReplacerConfig> fieldsToReplaceIfNull;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "Field Does Not Exist",
    description="Action for data that does not contain the specified fields",
    displayPosition = 30,
    group = "REPLACE"
  )
  @ValueChooser(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @Override
  protected Processor createProcessor() {
    return new FieldValueReplacerProcessor(fieldsToNull, fieldsToReplaceIfNull, onStagePreConditionFailure);
  }
}