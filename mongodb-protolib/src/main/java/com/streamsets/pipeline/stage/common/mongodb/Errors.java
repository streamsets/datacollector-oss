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
package com.streamsets.pipeline.stage.common.mongodb;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MONGODB_00("Failed to create MongoClientURI: {}"),
  MONGODB_01("Failed to create MongoClient: {}"),
  MONGODB_02("Failed to get database: '{}'. {}"),
  MONGODB_03("Failed to get collection: '{}'. {}"),
  MONGODB_04("Collection isn't tailable because '{}' is not a capped collection."),
  MONGODB_05("Offset Field '{}' must be an instance of {}"),
  MONGODB_06("Error retrieving documents from collection: '{}'. {}"),
  MONGODB_07("Failed to get <host:port> for '{}'"),
  MONGODB_08("Failed to parse port: '{}'"),
  MONGODB_09("Unknown host: '{}'"),
  MONGODB_10("Failed to parse entry: {}"),
  MONGODB_11("Offset tracking field: '{}' missing from document: '{}'"),
  MONGODB_12("Error writing to database: {}"),
  MONGODB_13("Error serializing record '{}': {}"),
  MONGODB_14("Unsupported operation type '{}' found in record {}"),
  MONGODB_15("Operation type (insert, update or delete) is not specified in the header for record {}"),
  MONGODB_16("Record {} does not contain the expected unique key field {}"),
  MONGODB_17("Error writing records to Mongo : {}"),
  MONGODB_18("Operation '{}' requires unique key to be configured"),
  MONGODB_19("Initial Offset is required when the offset field is ObjectId type"),
  MONGODB_20("Unknown Offset type : {}"),
  MONGODB_21("Error parsing {} to Date"),

  MONGODB_30("Oplog Document Missing the follow mandatory fields '{}'"),
  MONGODB_31("Oplog Offset Invalid, Cannot parse offset '{}'," +
      " offset should be of the form 'time_t::ordinal'. Reason {}"),
  MONGODB_32("Invalid Initial Offset Value for '{}', should be greater than -1 if '{}' is not -1"),
  MONGODB_33("Invalid Oplog Collection Name '{}', Oplog collection should start with 'oplog.'"),
  MONGODB_34("Can't create credential object: {}"),

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
