/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HIVE_00("Cannot have multiple field mappings for the same column: '{}'"),
  HIVE_01("Error: {}"),
  HIVE_02("Schema '{}' does not exist."),
  HIVE_03("Table '{}.{}' does not exist."),
  HIVE_04("Thrift protocol error: {}"),
  HIVE_05("Hive Metastore error: {}"),
  HIVE_06("Configuration file '{}' is missing from '{}'"),
  HIVE_07("Configuration dir '{}' does not exist"),
  HIVE_08("Partition field paths '{}' missing from record"),
  HIVE_09("Hive Streaming Error: {}"),
  HIVE_11("Failed to get login user"),
  HIVE_12("Failed to create Hive Endpoint: {}"),
  HIVE_13("Hive Metastore Thrift URL or Hive Configuration Directory is required."),
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
