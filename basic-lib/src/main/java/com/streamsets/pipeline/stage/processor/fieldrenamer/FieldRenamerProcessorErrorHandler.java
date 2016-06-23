/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
      label = "Handle From Field Does Not Exist",
      description="Action for data that does not contain the specified From Fields",
      displayPosition = 10,
      group = "RENAME"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Handle To Field Already Exist",
      description="Action for data that contains the specified To Fields",
      displayPosition = 20,
      group = "RENAME"
  )
  @ValueChooserModel(ExistingFieldHandlingChooserValues.class)
  public ExistingToFieldHandling existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Handle Multiple From Fields Match",
      description="Action for multiple From Field Expressions matching the same from Field",
      displayPosition = 30,
      group = "RENAME"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;

}
