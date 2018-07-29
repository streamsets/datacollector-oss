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
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HIVE_METADATA_01("Value Expression for partition value is missing"),
  HIVE_METADATA_02("Record is missing necessary data {}"),
  HIVE_METADATA_03("Failed validation on {} : Invalid character for Hive '{}'"),
  HIVE_METADATA_04("Invalid time basis expression '{}': {}"),
  HIVE_METADATA_05("{} is missing in the configuration file"),
  HIVE_METADATA_06("{} is required for external table"),
  HIVE_METADATA_07("Invalid value {} for {} in field {}, minimum: {}, maximum: 38"),
  HIVE_METADATA_08("Invalid value {} for scale in field {}, should be less than or equal to precision's value: {}"),
  HIVE_METADATA_09("Invalid type for partition: {}"),
  HIVE_METADATA_10("Unsupported character to use for partition value: {}"),
  HIVE_METADATA_11("Invalid comment for column '{}': {}"),
  HIVE_METADATA_12("Cannot evaluate expression '{}' for record '{}': {}")
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
