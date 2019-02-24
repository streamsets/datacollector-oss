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
package com.streamsets.pipeline.lib.generator.avro;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  AVRO_GENERATOR_00("Record '{}' is missing required avro field '{}'"),
  AVRO_GENERATOR_01("Error getting default value for avro field '{}' : {}"),
  AVRO_GENERATOR_02("Expected a default value of 'null' but found '{}'"),
  AVRO_GENERATOR_03("Record '{}' is missing required header 'avroSchema'"),
  AVRO_GENERATOR_04("RecordRecord {} has a different schema than is used for the current file. Current schema is '{}' whereas the record schema is '{}'"),
  AVRO_GENERATOR_05("Field type mismatch: Schema field type '{}' Record field type '{}'"),
  AVRO_GENERATOR_06("Can't store value '{}' of type {} in schema '{}': {}"),
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
