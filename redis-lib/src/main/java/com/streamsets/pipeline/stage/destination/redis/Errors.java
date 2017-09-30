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
package com.streamsets.pipeline.stage.destination.redis;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  REDIS_01("Failed to connect to Redis on '{}' : {}'"),
  REDIS_02("Failed to parse URI '{}' : {}"),
  REDIS_03("Failed to operate '{}' operation against a key '{}' : '{}'"),
  REDIS_04("Unsupported operation type '{}' found in record {}"),
  REDIS_05("Unsupported data type '{}'"),
  REDIS_06("Failed to publish"),
  REDIS_07("Unsupported '{}' : '{}' found in record {}"),
  REDIS_08("Error while writing records to redis"),
  REDIS_09("Delete error. Key cannot not be null or empty."),
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
