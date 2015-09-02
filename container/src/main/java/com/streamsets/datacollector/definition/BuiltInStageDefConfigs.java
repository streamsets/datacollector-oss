/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.OnRecordErrorChooserValues;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.List;

@GenerateResourceBundle
@ConfigGroups(PipelineGroups.class)
public abstract class BuiltInStageDefConfigs implements Stage {

  public static final String STAGE_ON_RECORD_ERROR_CONFIG = "stageOnRecordError";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "On Record Error",
      description = "Action to take with records sent to error",
      displayPosition = 10
  )
  @ValueChooserModel(OnRecordErrorChooserValues.class)
  public OnRecordError stageOnRecordError;

  public static final String STAGE_REQUIRED_FIELDS_CONFIG = "stageRequiredFields";
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Required Fields",
      description = "Records without any of these fields are sent to error",
      displayPosition = 20
  )
  @FieldSelectorModel(singleValued = false)
  public List<String> stageRequiredFields;

  public static final String STAGE_PRECONDITIONS_CONFIG = "stageRecordPreconditions";
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      label = "Preconditions",
      description = "Records that don't satisfy all the preconditions are sent to error",
      displayPosition = 30,
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public List<String> stageRecordPreconditions;

}
