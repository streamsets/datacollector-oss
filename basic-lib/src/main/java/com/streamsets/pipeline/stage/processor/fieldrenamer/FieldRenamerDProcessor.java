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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
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
    label="Field Renamer",
    description = "Rename fields",
    icon="edit.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldRenamerDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Rename",
      description = "Fields to rename, and target field names.",
      displayPosition = 40,
      group = "RENAME"
  )
  @ListBeanModel
  public List<FieldRenamerConfig> renameMapping;

  @ConfigDef(
    required = true,
    type = Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "Field Does Not Exist",
    description="Action for data that does not contain the specified fields",
    displayPosition = 30,
    group = "RENAME"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @ConfigDef(
      required = true,
      type = Type.BOOLEAN,
      defaultValue = "FALSE",
      label = "Overwrite Existing Fields",
      description="Whether or not to overwrite fields if a target field already exists",
      displayPosition = 30,
      group = "RENAME"
  )
  public boolean overwriteExisting;

  @Override
  protected Processor createProcessor() {
    return new FieldRenamerProcessor(renameMapping, onStagePreConditionFailure, overwriteExisting);
  }
}
