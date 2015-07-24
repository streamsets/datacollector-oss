/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmerger;

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

@StageDef(
    version=1,
    label="Field Merger",
    description = "Merge fields of like types",
    icon="merge.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldMergerDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields to merge",
      description = "Fields to merge, and fields to merge into.",
      displayPosition = 40,
      group = "MERGE"
  )
  @ComplexField(FieldMergerConfig.class)
  public List<FieldMergerConfig> mergeMapping;

  @ConfigDef(
    required = true,
    type = Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "Source Field Does Not Exist",
    description="Action for data that does not contain the specified source field",
    displayPosition = 30,
    group = "MERGE"
  )
  @ValueChooser(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @ConfigDef(
      required = true,
      type = Type.BOOLEAN,
      defaultValue = "FALSE",
      label = "Overwrite Fields",
      description="Whether or not to overwrite fields if a target field already exists",
      displayPosition = 30,
      group = "MERGE"
  )
  public boolean overwriteExisting;

  @Override
  protected Processor createProcessor() {
    return new FieldMergerProcessor(mergeMapping, onStagePreConditionFailure, overwriteExisting);
  }
}