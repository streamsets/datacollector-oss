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
package com.streamsets.pipeline.stage.processor.kv.redis;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  REDIS_LOOKUP_01("Failed to connect to Redis on '{}' : {}'"),
  REDIS_LOOKUP_02("Failed to parse URI '{}' : {}"),
  REDIS_LOOKUP_03("Failed to operate against a key '{}' : {}"),
  REDIS_LOOKUP_04("Unsupported operation type '{}' found in record {}"),
  LOOKUP_01("Failed to evaluate expression: '{}'"),
  LOOKUP_02("Failed to fetch values for batch: '{}'"),
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
