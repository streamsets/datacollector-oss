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
package com.streamsets.pipeline.stage.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.config.AvroSchemaLookupMode;

@GenerateResourceBundle
public enum DataFormatErrors implements ErrorCode {
  // Configuration errors
  DATA_FORMAT_01("Max data object length cannot be less than 1"),
  DATA_FORMAT_03("Invalid XML element name or XPath expression '{}': {}"),
  DATA_FORMAT_04("Unsupported data format '{}'"),
  DATA_FORMAT_05("Unsupported charset '{}'"),
  DATA_FORMAT_06("Cannot create the parser factory: {}"),
  DATA_FORMAT_07("Protobuf Descriptor File (.desc) location must be specified. It must be relative to the resources directory"),
  DATA_FORMAT_08("Message type must be specified"),
  DATA_FORMAT_09("Protobuf Descriptor File '{}' does not exist"),
  DATA_FORMAT_10("Error getting descriptor for message '{}' using protobuf descriptor file '{}', reason: {}"),
  DATA_FORMAT_11(AvroSchemaLookupMode.AUTO.getLabel() + " is not supported for this stage"),

  DATA_FORMAT_12("Data format must be specified"),
  DATA_FORMAT_200("Field cannot be empty"),
  DATA_FORMAT_201("Cannot create the parser factory: {}"),

  DATA_FORMAT_300("Error validating avro schema : {}"),
  DATA_FORMAT_301("Error getting default values from avro schema : {}"),

  DATA_FORMAT_302("Input data is not Base64 for record: {}"),

  DATA_FORMAT_303("Could not parse XML object '{}'"),
  DATA_FORMAT_304("Unmapped XPath namespace prefixes defined in record separator {}"),

  DATA_FORMAT_400("collectd Types DB '{}' not found"),
  DATA_FORMAT_401("collectd Auth File '{}' not found"),

  DATA_FORMAT_500("Line separator can only be either 1 or two characters, given '{}' ({} characters)"),
  ;
  private final String msg;

  DataFormatErrors(String msg) {
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
