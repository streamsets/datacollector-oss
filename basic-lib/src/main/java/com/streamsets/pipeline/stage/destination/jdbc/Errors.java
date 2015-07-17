/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.jdbc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  // Configuration errors
  JDBCDEST_00("Cannot connect to specified database: {}"),
  JDBCDEST_01("Failed to create JDBC driver, JDBC driver JAR may be missing: {}"),
  JDBCDEST_02("Failed to insert record: '{}' Error: {}"),
  JDBCDEST_03("Failed to initialize connection pool: {}"),
  JDBCDEST_04("Invalid column mapping: {}"),
  JDBCDEST_05("Unsupported data type in record: {}"),
  JDBCDEST_06("Failed to prepare insert statement: {}"),
  JDBCDEST_07("JDBC connection cleanup failed: {}")
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
