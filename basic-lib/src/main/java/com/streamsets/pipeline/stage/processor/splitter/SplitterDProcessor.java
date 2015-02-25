/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
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

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Field Splitter",
    description = "Splits a string field based on a separator character",
    icon="splitter.png"
)
@ConfigGroups(Groups.class)
public class SplitterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Split",
      description = "",
      displayPosition = 10,
      group = "FIELD_SPLITTER"
  )
  @FieldSelector(singleValued = true)
  public String fieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CHARACTER,
      defaultValue = " ",
      label = "Separator",
      description = "A single character",
      displayPosition = 20,
      group = "FIELD_SPLITTER"
  )
  public char separator;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      label = "New Split Fields",
      description="New fields to pass split data. The last field includes any remaining unsplit data.",
      displayPosition = 30,
      group = "FIELD_SPLITTER"
  )
  public List<String> fieldPathsForSplits;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Not Enough Splits ",
      description="Action for data that cannot be split as configured",
      displayPosition = 40,
      group = "FIELD_SPLITTER"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "REMOVE",
      label = "Original Field",
      description="Action for the original field being split",
      displayPosition = 50,
      group = "FIELD_SPLITTER"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = OriginalFieldActionChooserValues.class)
  public OriginalFieldAction originalFieldAction;

  @Override
  protected Processor createProcessor() {
    return new SplitterProcessor(fieldPath, separator, fieldPathsForSplits, onStagePreConditionFailure,
                                 originalFieldAction);
  }

}
