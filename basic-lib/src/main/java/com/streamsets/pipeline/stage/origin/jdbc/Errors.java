/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  // Configuration errors
  JDBC_00("Cannot connect to specified database: {}"),
  JDBC_01("Failed to create JDBC driver, JDBC driver JAR may be missing: {}"),
  JDBC_02("Offset column '{}' not found in query '{}' results."),
  JDBC_03("Failed to parse column '{}' to field with value {}."),
  JDBC_04("Query is failed to execute: '{}' Error: {}"),
  JDBC_05("Query must include '{}' in WHERE clause and in ORDER BY clause before other columns."),
  JDBC_07("Failed to load JDBC driver. If driver is not JDBC 4 compliant you must specify 'Driver Class Name' in" +
      "the Legacy configuration tab. Error: {}"),
  JDBC_09("The query interval expression must be greater than or equal to zero."),
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