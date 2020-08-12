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
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;

@StageDef(
    version=4,
    label="Field Hasher",
    description = "Uses an algorithm to hash field values",
    icon="hash.png",
    upgrader = FieldHasherProcessorUpgrader.class,
    upgraderDef = "upgrader/FieldHasherDProcessor.yaml",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    onlineHelpRefUrl ="index.html?contextID=task_xjd_dlk_wq"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldHasherDProcessor extends DProcessor {

  @ConfigDefBean(groups = {"FIELD_HASHING", "RECORD_HASHING"})
  public HasherConfig hasherConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "On Field Issue",
    description="Action for data that does not contain the specified fields, the field value is null or if the " +
      "field type is Map or List",
    displayPosition = 20,
    group = "FIELD_HASHING"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @Override
  protected Processor createProcessor() {
    return new FieldHasherProcessor(hasherConfig, onStagePreConditionFailure);
  }

}
