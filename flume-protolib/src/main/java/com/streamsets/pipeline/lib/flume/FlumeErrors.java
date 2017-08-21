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
package com.streamsets.pipeline.lib.flume;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum FlumeErrors implements ErrorCode {
  //Flume Origin

  //Flume target messages
  FLUME_50("Error serializing record '{}': {}"),
  FLUME_51("Unable to send data to flume, reason : {}"),
  FLUME_52("Unable to send data to flume because the pipeline was stopped."),

  //Flume ValidationValidation
  FLUME_101("Flume Host Configuration cannot be empty"),
  FLUME_102("Flume Host Alias cannot be empty"),
  FLUME_103("Invalid Flume Host Configuration '{}'"),
  FLUME_104("Configuration '{}' cannot be less than '{}'"),

  ;
  private final String msg;

  FlumeErrors(String msg) {
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
