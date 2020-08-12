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
package com.streamsets.pipeline.stage.processor.fieldmerger;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
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
    version=1,
    label="Field Merger",
    description = "Merge fields of like types",
    icon="merge.png",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/FieldMergerDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_ghx_5vl_gt"
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
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MERGE"
  )
  @ListBeanModel
  public List<FieldMergerConfig> mergeMapping;

  @ConfigDef(
    required = true,
    type = Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "Source Field Does Not Exist",
    description="Action for data that does not contain the specified source field",
    displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
    group = "MERGE"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @ConfigDef(
      required = true,
      type = Type.BOOLEAN,
      defaultValue = "FALSE",
      label = "Overwrite Fields",
      description="Whether or not to overwrite fields if a target field already exists",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MERGE"
  )
  public boolean overwriteExisting;

  @Override
  protected Processor createProcessor() {
    return new FieldMergerProcessor(mergeMapping, onStagePreConditionFailure, overwriteExisting);
  }
}
