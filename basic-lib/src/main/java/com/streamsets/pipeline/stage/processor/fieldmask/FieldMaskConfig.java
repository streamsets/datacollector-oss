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
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.List;

public class FieldMaskConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Mask",
      description="Mask string fields. You can enter multiple fields for the same mask type.",
      displayPosition = 10
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="VARIABLE_LENGTH",
      label = "Mask Type",
      description="",
      displayPosition = 20
  )
  @ValueChooserModel(MaskTypeChooseValues.class)
  public MaskType maskType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Custom Mask",
      description = "Use # to reveal field values. Other characters replace field values.",
      displayPosition = 30,
      dependsOn = "maskType",
      triggeredByValue = "CUSTOM"
  )
  public String mask;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Regular Expression",
    description = "Regular expression that matches the data and groups them.",
    displayPosition = 40,
    dependsOn = "maskType",
    triggeredByValue = "REGEX",
    defaultValue = "(.*)"

  )
  public String regex;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Groups To Show",
    description = "Comma separated list of group numbers that must be revealed in the data.",
    displayPosition = 50,
    dependsOn = "maskType",
    triggeredByValue = "REGEX",
    defaultValue = "1"
  )
  public String groupsToShow;

}
