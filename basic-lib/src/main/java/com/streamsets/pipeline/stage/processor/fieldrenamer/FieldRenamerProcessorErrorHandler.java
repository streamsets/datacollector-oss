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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;

public class FieldRenamerProcessorErrorHandler {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Source Field Does Not Exist",
      description="Response when records do not include the specified source fields.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RENAME"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Target Field Already Exists",
      description="Response when records include field names that match the specified target fields.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RENAME"
  )
  @ValueChooserModel(ExistingFieldHandlingChooserValues.class)
  public ExistingToFieldHandling existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Multiple Source Field Matches",
      description="Response when source fields match multiple source field regular expressions.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RENAME"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;

}
