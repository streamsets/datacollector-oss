/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;
import com.streamsets.pipeline.configurablestage.DProcessor;

@StageDef(
    version=1,
    label="List Pivoter",
    description = "Produce new records for each element of a list",
    icon="pivoter.png",
    upgrader = ListPivotProcessorUpgrader.class,
    onlineHelpRefUrl = "index.html#Processors/ListPivoter.html#task_dn1_k13_qw"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class ListPivotDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      group = "PIVOT",
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "List To Pivot",
      description = "Path to List-type field that will be exploded into multiple records.",
      displayPosition = 10
  )
  @FieldSelectorModel(singleValued = true)
  public String listPath;

  @ConfigDef(
      required = true,
      group = "PIVOT",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Copy Other Fields",
      description = "Copy all other fields to each resulting record.",
      displayPosition = 20
  )
  public boolean copyFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Field Does Not Exist",
      description="Action for data that does not contain the specified fields",
      displayPosition = 30,
      group = "PIVOT"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @Override
  protected Processor createProcessor() {
    return new ListPivotProcessor(
        listPath,
        copyFields,
        onStagePreConditionFailure
    );
  }
}
