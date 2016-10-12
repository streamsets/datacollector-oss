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
package com.streamsets.pipeline.stage.processor.fieldflattener;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.LinkedList;
import java.util.List;

public class FieldFlattenerConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="ENTIRE_RECORD",
    label = "Flatten",
    description = "Select what should be flattened in the record",
    displayPosition = 10,
    group = "FLATTEN"
  )
  @ValueChooserModel(FlattenTypeChooserValues.class)
  public FlattenType flattenType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.LIST,
    label = "Fields",
    dependsOn = "flattenType",
    description = "List of fields to be flattened",
    displayPosition = 15,
    group = "FLATTEN",
    triggeredByValue = { "SPECIFIC_FIELDS" }
  )
  public List<String> fields = new LinkedList<>();

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Name separator",
    defaultValue = ".",
    description = "Separator that is used when created merged field name from nested structures.",
    displayPosition = 20,
    group = "FLATTEN"
  )
  public String nameSeparator;

}
