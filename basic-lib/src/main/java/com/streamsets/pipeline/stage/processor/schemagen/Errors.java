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
package com.streamsets.pipeline.stage.processor.schemagen;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SCHEMA_GEN_0001("Can't instantiate Schema generator class: {}"),
  SCHEMA_GEN_0002("Unsupported type: {}"),
  SCHEMA_GEN_0003("Unsupported type {} for root field."),
  SCHEMA_GEN_0004("Invalid scale or precision of value {} for field {}"),
  SCHEMA_GEN_0005("List '{}' have different schemas for items. First schema: '{}', Second schema: '{}'"),
  SCHEMA_GEN_0006("Can't generate Avro schema for empty list '{}'"),
  SCHEMA_GEN_0007("Map '{}' have different schemas for items. First schema: '{}', Second schema: '{}'"),
  SCHEMA_GEN_0008("Can't generate Avro schema for null or empty map '{}'"),
  SCHEMA_GEN_0009("Multiple default values for type: {}"),
  SCHEMA_GEN_0010("Can't generate schema for record '{}': {}"),
  ;

  private final String label;

  Errors(String label) {
    this.label = label;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return label;
  }
}
