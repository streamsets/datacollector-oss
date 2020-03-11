/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.mysql.error;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum MySQLBinLogErrors implements ErrorCode {

  MYSQL_BIN_LOG_001("Error fetching data from MySql: {}"),
  MYSQL_BIN_LOG_002("Table {}.{} metadata not found"),
  MYSQL_BIN_LOG_003("Error connecting to MySql: {}"),
  MYSQL_BIN_LOG_004("Error processing MySql event {} at offset {}: {}"),
  MYSQL_BIN_LOG_006("MySql server error: {}"),
  MYSQL_BIN_LOG_007("Ignore tables format error: {}"),
  MYSQL_BIN_LOG_008("Include tables format error: {}"),
  MYSQL_BIN_LOG_009("Error disconnecting from MySQL: {}"),
  MYSQL_BIN_LOG_010("Error creating include tables filter: {}"),
  MYSQL_BIN_LOG_011("Unable to parse initial offset: {}. It must be of the format <file>:<position>"),
  MYSQL_BIN_LOG_012("Unhandled communication error: {}"),
  MYSQL_BIN_LOG_013("Error deserializing event: {}"),
  MYSQL_BIN_LOG_014("BinaryLogClient server error: {}"),
  MYSQL_BIN_LOG_015("Error creating include tables filter: {}"),
  MYSQL_BIN_LOG_016("Error creating ignore tables filter: {}"),
  MYSQL_BIN_LOG_017("Error disconnecting from MySql"),
  MYSQL_BIN_LOG_018("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
  ;

  private final String msg;

  MySQLBinLogErrors(String msg) {
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
