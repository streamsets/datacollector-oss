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
package com.streamsets.pipeline.stage.util.scripting;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SCRIPTING_00("Scripting engine '{}' not found"),
  SCRIPTING_01("Could not load scripting engine '{}': {}"),
  SCRIPTING_02("Script cannot be empty"),
  SCRIPTING_03("Script failed to compile: '{}'"),
  SCRIPTING_04("Script sent record to error: {}"),
  SCRIPTING_05("Script error while processing record: {}"),
  SCRIPTING_06("Script error while processing batch: {}"),
  SCRIPTING_07("Sending normal record to event stream: {}"),
  SCRIPTING_08("Script error while running init script: {}"),
  SCRIPTING_09("Script error while running destroy script: {}"),
  SCRIPTING_10("Script error in user script: {}"),
  SCRIPTING_11("{} while trying to close engine: {}"),
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
