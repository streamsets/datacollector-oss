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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  REMOTE_DOWNLOAD_01("Failed to process file '{}' at position '{}': {}"),
  REMOTE_DOWNLOAD_02("Failed to read data from file '{}' at position '{}' due to: {}"),
  REMOTE_DOWNLOAD_03("Failed to read data due to: {}"),

  REMOTE_DOWNLOAD_04("File Pattern cannot be empty"),
  REMOTE_DOWNLOAD_05("Invalid {} file pattern '{}': {}"),
  REMOTE_DOWNLOAD_06("Initial file '{}' is invalid: {}"),
  REMOTE_DOWNLOAD_07("Archive directory cannot be empty"),
  REMOTE_DOWNLOAD_08("Problem setting archive directory: {}"),
  REMOTE_DOWNLOAD_09("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
  REMOTE_DOWNLOAD_10("Cannot read file {}"),
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
