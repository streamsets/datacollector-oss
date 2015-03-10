/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Field Hasher",
    description = "Uses an algorithm to hash field values",
    icon="hash.png")
@ConfigGroups(Groups.class)
public class FieldHasherDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "",
      description="",
      displayPosition = 10,
      group = "HASHING"
  )
  @ComplexField
  public List<FieldHasherConfig> fieldHasherConfigs;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "On Field Issue",
    description="Action for data that does not contain the specified fields, the field value is null or if the " +
      "field type is Map or List",
    displayPosition = 20,
    group = "HASHING"
  )
  @ValueChooser(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @Override
  protected Processor createProcessor() {
    return new FieldHasherProcessor(fieldHasherConfigs, onStagePreConditionFailure);
  }

}
