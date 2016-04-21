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
  JDBCDEST_04("Invalid column mapping from field '{}' to column '{}'"),
  JDBCDEST_05("Unsupported data type in record: {}"),
  JDBCDEST_06("Failed to prepare insert statement: {}"),
  JDBCDEST_07("JDBC connection cleanup failed: {}"),
  JDBCDEST_08("Record missing required field {} for change log type {}"),
  JDBCDEST_09("Invalid operation '{}' for change log type {}"),
  JDBCDEST_10("Failed to create default column mappings for table '{}'. Check log for details."),
  JDBCDEST_11("Missing field '{}' in record for column '{}'"),
  JDBCDEST_12("Batch insertion error: {}"),
  JDBCDEST_13("Configuration of record writer failed: {}"),
  JDBCDEST_14("Error processing batch.\n{}"),
  JDBCDEST_15("Table name has more than 1 period, which is invalid: '{}'"),
  JDBCDEST_16("Table '{}' does not exist."),
  JDBCDEST_17("Failed to lookup primary keys for table '{}' : {}"),
  JDBCDEST_18("Table '{}' does not have a primary key, but it is required."),
  JDBCDEST_19("Record did not contain primary key field mapped to primary key column '{}'"),
  JDBCDEST_20("Invalid table name template expression '{}': {}"),
  JDBCDEST_22("The record had no fields that matched the columns in the destination table."),
  JDBCDEST_23("The field '{}' of type '{}' doesn't match the destination column's type."),
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
