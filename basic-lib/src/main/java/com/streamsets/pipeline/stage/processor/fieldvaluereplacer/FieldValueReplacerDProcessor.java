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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

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
    version=3,
    label="Value Replacer",
    description = "Replaces null values with a constant and replaces values with NULL",
    icon="replacer.png",
    onlineHelpRefUrl ="index.html?contextID=task_ihq_ymf_zq",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgrader = FieldValueReplacerUpgrader.class,
    upgraderDef = "upgrader/FieldValueReplacerDProcessor.yaml"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@Deprecated // Use FieldReplacerProcessor that is based on field path expression rather then set of difficult conditions
public class FieldValueReplacerDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields to Null",
      description="Conditionally Replaces existing values with null values",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REPLACE"
  )
  @ListBeanModel
  public List<NullReplacerConditionalConfig> nullReplacerConditionalConfigs;

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "Replace Null Values",
      description="Replaces the null values in a field with a specified value.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REPLACE"
  )
  @ListBeanModel
  public List<FieldValueReplacerConfig> fieldsToReplaceIfNull;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Field Does Not Exist",
      description="Action for data that does not contain the specified fields",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REPLACE"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "Conditionally Replace Values",
      description="Conditionally replaces values with new specified value.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REPLACE"
  )
  @ListBeanModel
  public List<FieldValueConditionalReplacerConfig> fieldsToConditionallyReplace;

  @Override
  protected Processor createProcessor() {
    return new FieldValueReplacerProcessor(
        nullReplacerConditionalConfigs,
        fieldsToReplaceIfNull,
        fieldsToConditionallyReplace,
        onStagePreConditionFailure
    );
  }
}
