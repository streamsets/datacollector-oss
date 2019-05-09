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
package com.streamsets.datacollector.restapi;

import com.streamsets.pipeline.api.ErrorCode;

public enum RestErrors implements ErrorCode {
  // 1xxx is for StageLibraryResource
  REST_1000("Stage library {} version {} requires SDC version at least {} (current version is {})"),
  REST_1001("Unable to find to find following stage libraries in repository list: {}"),
  REST_1002("Stage library {} already installed on version {}"),
  REST_1003("Failed to create directory: {}"),
  REST_1004("Environment variable 'STREAMSETS_LIBRARIES_EXTRA_DIR' is not set"),
  REST_1005("Invalid library name '{}'"),
  ;

  private final String message;

  RestErrors(String message) {
    this.message = message;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
