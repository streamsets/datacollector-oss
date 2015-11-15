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
  JDBC_06("Failed to initialize connection pool: {}"),
  JDBC_07("The query interval expression must be greater than or equal to zero."),
  JDBC_08("Query result has duplicate column label '{}'. Create an alias using 'AS' in your query."),
  JDBC_09("Offset Column '{}' cannot contain a '.' or prefix."),
  JDBC_10("'{}' is less than the minimum value of '{}'"),
  JDBC_11("Minimum Idle Connections ({}) must be less than or equal to Maximum Pool Size ({})"),
  JDBC_12("The JDBC driver for this database does not support scrollable cursors, " +
      "which are required when Transaction ID Column Name is specified."),
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