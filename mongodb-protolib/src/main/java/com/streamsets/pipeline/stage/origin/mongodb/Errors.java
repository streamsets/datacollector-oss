/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MONGODB_01("Failed to create MongoClient: {}"),
  MONGODB_02("Failed to get database: '{}'. {}"),
  MONGODB_03("Failed to get collection: '{}'. {}"),
  MONGODB_04("Collection isn't tailable because '{}' is not a capped collection."),
  MONGODB_05("Offset Field '{}' must be an instance of ObjectId"),
  MONGODB_06("Error retrieving documents from collection: '{}'. {}"),
  MONGODB_07("Failed to get <host:port> for '{}'"),
  MONGODB_08("Failed to parse port: '{}'"),
  MONGODB_09("Unknown host: '{}'"),
  MONGODB_10("Failed to parse entry: {}"),
  MONGODB_11("Offset tracking field: '{}' missing from document: '{}'"),
  MONGODB_12("Error writing to database: {}")

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
