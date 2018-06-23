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
package com.streamsets.pipeline.stage.lib.kudu;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  KUDU_00("Error connecting to kudu master '{}': {}"),
  KUDU_01("Table '{}' does not exist"),
  KUDU_02("Parameter is not valid"),
  KUDU_03("Errors while interacting with Kudu: {}"),
  KUDU_04("Column or field '{}' is not type '{}'"),
  KUDU_05("Column '{}' does not exist"),
  KUDU_06("Column '{}' mapped from field '{}' is not nullable"),
  KUDU_08("Row '{}' already exists"),
  KUDU_09("Field '{}' does not match destination type '{}': {}"),
  KUDU_10("Column/field '{}' is type '{}' which doesn't have an associated StreamSets type"),
  KUDU_11("Stage not initialized correctly, cannot write batch"),
  KUDU_12("Invalid table name template expression '{}': {}"),
  KUDU_13("Operation not supported: {}"),
  KUDU_14("Unknown action for unsupported operation: {}"),
  KUDU_15("Row key '{}' not found"),

  KUDU_30("Column mappings must be specified for lookup processor"),
  KUDU_31("No rows found"),
  KUDU_32("Primary key field '{}' not found in record"),
  KUDU_33("Unsupported column type: {}"),
  KUDU_34("Primary key '{}' is not configured in Key Column Mapping"),
  KUDU_35("Missing a value for column '{}'"),
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
