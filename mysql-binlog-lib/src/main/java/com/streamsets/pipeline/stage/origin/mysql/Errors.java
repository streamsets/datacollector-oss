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
package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MYSQL_001("Error fetching data from MySql: {}"),
  MYSQL_002("Table {}.{} metadata not found"),
  MYSQL_003("Error connecting to MySql: {}"),
  MYSQL_004("Error processing MySql event {} at offset {}: {}"),
  MYSQL_006("MySql server error: {}"),
  MYSQL_007("Ignore tables format error: {}"),
  MYSQL_008("Include tables format error: {}"),
  MYSQL_009("Couldn't parse JSON column value: {}"),
  MYSQL_010("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
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
