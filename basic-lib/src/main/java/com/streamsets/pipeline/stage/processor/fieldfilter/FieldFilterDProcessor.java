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
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;

@StageDef(
    version=1,
    label="Field Remover",
    description="Removes fields from a record",
    icon="filter.png",
    upgraderDef = "upgrader/FieldFilterDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_c1j_btr_wq",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH

    }
)
@ConfigGroups(Groups.class)
@HideConfigs(onErrorRecord = true)
@GenerateResourceBundle
public class FieldFilterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="REMOVE",
      label = "Action",
      description = "",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILTER"
  )
  @ValueChooserModel(FilterOperationChooserValues.class)
  public FilterOperation filterOperation;

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields",
      description = "",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILTER",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, FieldEL.class}
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = Type.STRING,
      defaultValue = "",
      label = "Constant",
      description = "",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILTER",
      dependsOn = "filterOperation",
      triggeredByValue = "REMOVE_CONSTANT"
  )
  public String constant;

  @Override
  protected Processor createProcessor() {
    return new FieldFilterProcessor(filterOperation, fields, constant);
  }

}
