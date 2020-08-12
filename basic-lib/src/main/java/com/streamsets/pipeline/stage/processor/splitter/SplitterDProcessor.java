/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;

import java.util.List;

@StageDef(
    version=2,
    label="Field Splitter",
    description = "Splits a string field based on a separator character",
    icon="splitter.png",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    onlineHelpRefUrl ="index.html?contextID=task_av1_5g3_yq",
    upgrader = SplitterProcessorUpgrader.class,
    upgraderDef = "upgrader/SplitterDProcessor.yaml"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SplitterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Split",
      description = "",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_SPLITTER"
  )
  @FieldSelectorModel(singleValued = true)
  public String fieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = " ",
      label = "Separator",
      description = "Regular expression to use for splitting the field. If trying to split on a RegEx meta" +
          " character \".$|()[{^?*+\\\", the character must be escaped with \\",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_SPLITTER"
  )
  public String separator;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[\"/fieldSplit1\", \"/fieldSplit2\"]",
      label = "New Split Fields",
      description="New fields to pass split data. The last field includes any remaining unsplit data.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_SPLITTER"
  )
  @FieldSelectorModel(singleValued = false)
  public List<String> fieldPathsForSplits;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Not Enough Splits",
      description="Action for data that has fewer splits than configured field paths",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_SPLITTER"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_LAST_FIELD",
      label = "Too Many Splits",
      description="Action for data that more splits than configured field paths",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_SPLITTER"
  )
  @ValueChooserModel(TooManySplitsActionChooserValues.class)
  public TooManySplitsAction tooManySplitsAction;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field for Remaining Splits",
      description = "List field used to store any remaining splits",
      displayPosition = 55,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "tooManySplitsAction",
      triggeredByValue = "TO_LIST",
      group = "FIELD_SPLITTER"
  )
  @FieldSelectorModel(singleValued = true)
  public String remainingSplitsPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "REMOVE",
      label = "Original Field",
      description="Action for the original field being split",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_SPLITTER"
  )
  @ValueChooserModel(OriginalFieldActionChooserValues.class)
  public OriginalFieldAction originalFieldAction;

  @Override
  protected Processor createProcessor() {
    return new SplitterProcessor(fieldPath, separator, fieldPathsForSplits, tooManySplitsAction, remainingSplitsPath,
                                 onStagePreConditionFailure, originalFieldAction);
  }

}
