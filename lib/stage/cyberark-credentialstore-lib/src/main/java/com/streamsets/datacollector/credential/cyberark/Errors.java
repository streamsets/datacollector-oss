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
package com.streamsets.datacollector.credential.cyberark;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  CYBERARCK_000("Store '{}' could not initialize CyberArk credential fetcher: {}"),
  CYBERARCK_001("Store '{}' couldn't retrieve credential '{}': {}"),
  CYBERARCK_002("Store '{}' failed to retrieve credential '{}', will wait '{}' ms"),
  CYBERARCK_003("Store '{}' error while making webservice call to '{}': {}"),
  CYBERARCK_004("Store '{}' URL '{}'. HTTP status '{}' message '{}'. CyberArk error code '{}' message '{}'"),
  ;

  private final String message;

  Errors(String message) {
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
