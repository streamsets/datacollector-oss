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
package com.streamsets.pipeline.stage.bigquery.lib;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  BIGQUERY_00("Query Job exceeded timeout."),
  BIGQUERY_02("Query Job execution error: '{}'"),
  BIGQUERY_04("Credentials file '{}' not found"),
  BIGQUERY_05("Error reading credentials file"),

  BIGQUERY_10("Error evaluating expression for the record. Reason : {}"),
  BIGQUERY_11("Error inserting record. Reasons : {}, Messages : {}"),
  BIGQUERY_12("Unsupported field '{}' of type '{}'"),
  BIGQUERY_13("Big Query insert failed. Reason : {}"),
  BIGQUERY_14("Empty row generated for the record"),
  BIGQUERY_15("Error evaluated Row Id, value evaluates to empty"),
  BIGQUERY_16("Root field of record should be a Map or a List Map"),
  BIGQUERY_17("Data set '{}' or Table '{}' does not exist in Big Query under project '{}'"),
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
