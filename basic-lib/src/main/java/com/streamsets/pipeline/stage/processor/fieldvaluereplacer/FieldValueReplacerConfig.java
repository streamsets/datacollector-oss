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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

import java.util.List;

public class FieldValueReplacerConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Replace",
      description = "You can enter multiple fields to replace with the same value",
      displayPosition = 10
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Replacement Value",
      description="Value to replace nulls",
      displayPosition = 20
  )
  public String newValue;

}
