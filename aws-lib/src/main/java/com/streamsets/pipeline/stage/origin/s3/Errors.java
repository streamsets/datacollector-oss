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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  S3_SPOOLDIR_01("Failed to process object '{}' at position '{}': {}"),
  S3_SPOOLDIR_02("Object with key '{}' at offset '{}' exceeds maximum length"),
  S3_SPOOLDIR_03("Object '{}' could not be fully processed, failed on '{}' offset: {}"),
  S3_SPOOLDIR_04("Buffer Limit must be equal or greater than {}KB and equal or less than {}MB"),
  S3_SPOOLDIR_06("File Pattern configuration is required"),
  S3_SPOOLDIR_07("Error Handling cannot be {} when Post Processing is {}"),

  S3_SPOOLDIR_10("Endpoint cannot be empty"),
  S3_SPOOLDIR_11("Bucket name cannot be empty"),
  S3_SPOOLDIR_12("Bucket '{}' does not exist"),
  S3_SPOOLDIR_13("Prefix cannot be empty"),
  S3_SPOOLDIR_14("Absolute source path cannot be same as the absolute post processing path, '{}'"),

  S3_SPOOLDIR_20("Cannot connect to Amazon S3, reason : {}"),
  S3_SPOOLDIR_21("Found invalid offset value '{}'"),
  S3_SPOOLDIR_23("Unable to fetch object, reason : {}"),
  S3_SPOOLDIR_24("Unable to move object, reason : {}"),
  S3_SPOOLDIR_25("Unable to get object content, reason : {}"),
  S3_SPOOLDIR_26("S3 runner failed. Reason {}"),
  S3_SPOOLDIR_27("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
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
