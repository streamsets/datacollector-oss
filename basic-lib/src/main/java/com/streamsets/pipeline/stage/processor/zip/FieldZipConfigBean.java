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
package com.streamsets.pipeline.stage.processor.zip;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;

import java.util.List;

public class FieldZipConfigBean {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Fields to Zip",
      description="Zips the specified fields into a single list.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ZIP"
  )
  @ListBeanModel
  public List<FieldZipConfig> fieldZipConfigs;


  @ConfigDef(
      required = true,
      group = "ZIP",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Zip Values Only",
      description = "If checked, the output will be a list of two-element lists. If unchecked, the output " +
          "will be a list of maps with each field value identified by the originating field name.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean valuesOnly;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Field Does Not Exist",
      description="Action for data that does not contain the specified fields",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ZIP"
  )
  @ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;
}
