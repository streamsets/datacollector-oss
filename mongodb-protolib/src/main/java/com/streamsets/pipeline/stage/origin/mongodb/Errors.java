/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  MONGODB_11("Offset tracking field: '{}' missing from document: '{}'")

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
