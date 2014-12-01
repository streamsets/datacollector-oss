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
package com.external.stage;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.FieldSelectionType;

import java.util.Map;

@StageDef(version="1.0", label="tc_processor"
  , description = "This processor lets the user select the types for the fields")
public class TypeConversionProcessor extends BaseProcessor {

  @FieldModifier(type = FieldSelectionType.PROVIDED, valuesProvider =TypesProvider.class)
  @ConfigDef(type= ConfigDef.Type.MODEL, defaultValue = "",
    required = true, label = "field_to_type_map", description = "Contains the field and its target type as chosen by the user")
  public Map<String, String> fieldToTypeMap;

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {

  }
}
