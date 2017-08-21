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
package com.streamsets.pipeline.lib.parser.protobuf;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  PROTOBUF_00("Could not find non-nested message type '{}' in Protobuf Descriptor File '{}'"),
  PROTOBUF_01("Error building FileDescriptor for '{}'. Could not satisfy dependencies."),
  PROTOBUF_02("Descriptor is for a map, but the SDC Record field was a {}"),
  PROTOBUF_03("Unknown Field Descriptor type '{}'"),
  PROTOBUF_04("Record '{}' is missing required protobuf field '{}'"),
  PROTOBUF_05("Error while serializing unknown fields: {}"),
  PROTOBUF_06("Field descriptor file '{}' does not exist."),
  PROTOBUF_07("Descriptor file is not valid: {}"),
  PROTOBUF_08("Error reading FileDescriptorSet: {}"),
  PROTOBUF_09("A MapEntry should only have key and value fields, but found {}"),
  PROTOBUF_10("Error while writing serialized message: {}"),
  PROTOBUF_11("Error while converting value '{}' to type '{}'"),
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
