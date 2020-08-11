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
package com.streamsets.datacollector.creation;


import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.config.OnRecordErrorChooserValues;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;
import java.util.Set;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public class StageConfigBean {

  public static final String STAGE_ON_RECORD_ERROR_CONFIG = "stageOnRecordError";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue= "TO_ERROR",
      label = "On Record Error",
      description = "Action to take with records sent to error",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = ""
  )
  @ValueChooserModel(OnRecordErrorChooserValues.class)
  public OnRecordError stageOnRecordError;

  public static final String STAGE_REQUIRED_FIELDS_CONFIG = "stageRequiredFields";
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Required Fields",
      description = "Records without any of these fields are sent to error",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = ""
  )
  @FieldSelectorModel
  public List<String> stageRequiredFields;

  public static final String STAGE_PRECONDITIONS_CONFIG = "stageRecordPreconditions";
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      defaultValue="[]",
      label = "Preconditions",
      description = "Records that don't satisfy all the preconditions are sent to error",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = { RecordEL.class, RuntimeEL.class }
  )
  public List<String> stageRecordPreconditions;

  public static final Set<String> CONFIGS = ImmutableSet.of(STAGE_ON_RECORD_ERROR_CONFIG, STAGE_PRECONDITIONS_CONFIG,
                                                            STAGE_REQUIRED_FIELDS_CONFIG);

}
