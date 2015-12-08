/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.splitter;

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

import java.util.List;

@StageDef(
    version=1,
    label="Field Splitter",
    description = "Splits a string field based on a separator character",
    icon="splitter.png",
    onlineHelpRefUrl = "index.html#Processors/FieldSplitter.html#task_av1_5g3_yq"
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
      group = "FIELD_SPLITTER"
  )
  @FieldSelectorModel(singleValued = true)
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
      defaultValue = "[\"/fieldSplit1\", \"/fieldSplit2\"]",
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
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
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
  @ValueChooserModel(OriginalFieldActionChooserValues.class)
  public OriginalFieldAction originalFieldAction;

  @Override
  protected Processor createProcessor() {
    return new SplitterProcessor(fieldPath, separator, fieldPathsForSplits, onStagePreConditionFailure,
                                 originalFieldAction);
  }

}
