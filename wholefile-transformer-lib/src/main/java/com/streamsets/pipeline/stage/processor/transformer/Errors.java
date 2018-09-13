/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.transformer;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  CONVERT_01("Failed to validate record is a whole file data format : {}"),
  CONVERT_02("Directory Path cannot be empty : {}"),
  CONVERT_03("File Name cannot be empty : {}"),
  CONVERT_04("Failed to evaluate expression : {}"),
  CONVERT_05("Directory Path should be absolute : {}"),
  CONVERT_06("Failed to delete old temporary file : {}"),
  CONVERT_07("Failed to get temp file info : {}"),
  CONVERT_08("Error while converting avro record: '{}' at {}"),
  CONVERT_09("Failed to generate header attrs"),
  CONVERT_10("Failed to get Directory Path : {}"),
  CONVERT_11("Failed to get avro file stream : {}"),

  ;

  private final String message;

  Errors(String message) {
    this.message = message;
  }
  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
