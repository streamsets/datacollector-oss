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
package com.streamsets.pipeline.stage.processor.listpivot;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;

@StageDef(
    version=2,
    label="Field Pivoter",
    description = "Produce new records for each element of a list or map field",
    icon="pivoter.png",
    upgrader = ListPivotProcessorUpgrader.class,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Processors/ListPivoter.html#task_dn1_k13_qw"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class ListPivotDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      group = "PIVOT",
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field To Pivot",
      description = "Path to the field that will be exploded into multiple records (supported types are LIST and LIST_MAP).",
      displayPosition = 10
  )
  @FieldSelectorModel(singleValued = true)
  public String listPath;

  @ConfigDef(
      required = true,
      group = "PIVOT",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Copy All Fields",
      description = "Copy all fields (including the original list) to each resulting record. " +
          "If this is not set, then the pivoted value is set as the root field of the record.",
      displayPosition = 20
  )
  public boolean copyFields;

  @ConfigDef(
      required = false,
      group = "PIVOT",
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      dependsOn = "copyFields",
      triggeredByValue = "true",
      label = "Pivoted Items Path",
      description = "Path in the new record where the pivoted list items are written to. Each record will contain one" +
          "item from the original list at this path. If this is not specified, the path of the original list is used. " +
          "If there is data at this field path, it will be overwritten.",
      displayPosition = 30
  )
  public String newPath;

  @ConfigDef(
      required = true,
      group = "PIVOT",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Save Original Field Name",
      description = "Specifies whether or not to save the original field name of the pivoted field.",
      displayPosition = 40
  )
  public boolean saveOriginalFieldName;

  @ConfigDef(
      required = true,
      group = "PIVOT",
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      dependsOn = "saveOriginalFieldName",
      triggeredByValue = "true",
      label = "Original Field Name Path",
      description = "Path in the new record to store the name of the field that was pivoted.",
      displayPosition = 41
  )
  public String originalFieldNamePath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Field Does Not Exist",
      description="Action for data that does not contain the specified fields",
      displayPosition = 50,
      group = "PIVOT"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @Override
  protected Processor createProcessor() {
    return new ListPivotProcessor(
        listPath,
        newPath,
        copyFields,
        saveOriginalFieldName,
        originalFieldNamePath,
        onStagePreConditionFailure
    );
  }
}
