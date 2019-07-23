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
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum CommonError implements ErrorCode {

  CMN_0100("Unsupported field type '{}' with value '{}' encountered in record '{}'"),
  CMN_0101("Error converting record '{}' to string: {}"),
  CMN_0102("Field Path to Column Name mapping must be configured to convert records to CSV."),
  CMN_0103("Error converting record '{}' to string: {}"),
  CMN_0104("Error evaluating expression {}: {}"),
  CMN_0105("Error parsing expression {}: {}"),
  CMN_0106("Error resolving union for SDC Type {} (java class {}) against schema {}: {}"),

  ;
  private final String msg;

  CommonError(String msg) {
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
