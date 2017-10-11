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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  FORCE_00("A configuration is invalid because: {}"),
  FORCE_01("Error creating Bulk API job: {}"),
  FORCE_02("Error getting Bulk API batch info: {}"),
  FORCE_03("Batch failed: {}"),
  FORCE_04("Record read failed: {}"),
  FORCE_05("Get query result failed: {}"),
  FORCE_06("Can't find offset column in result header: {}"),
  FORCE_07("SOQL query must include '{}' in WHERE clause and in ORDER BY clause before other columns."),
  FORCE_08("Error querying SOAP API: {}"),
  FORCE_09("Streaming API Error: {}"),
  FORCE_10("Thread interrupted: {}"),
  FORCE_11("Writing record id '{}' failed due to: {}"),
  FORCE_12("Invalid SObject name template expression '{}': {}"),
  FORCE_13("Error writing to Salesforce: {}"),
  FORCE_14("Failed to create delimited data generator : {}"),
  FORCE_15("No results for query: '{}'"),
  FORCE_16("Failed to evaluate expression: '{}'"),
  FORCE_17("Exception executing query: '{}' - '{}'"),
  FORCE_18("Since the default value of '{}' is not empty, its data type cannot be '" + DataType.USE_SALESFORCE_TYPE.getLabel() + "'."),
  FORCE_19("Error getting user info: {}"),
  FORCE_20("Failed to parse Salesforce field '{}' to SDC field with value {}."),
  FORCE_21("Can't get metadata for object: {}"),
  FORCE_22("Can't find offset column {} in the returned data. Ensure it is present in the query."),
  FORCE_23("Unsupported operation in record header: {}"),
  FORCE_24("Invalid External ID Field expression '{}': {}"),
  FORCE_25("Preview timed out before the origin retrieved data. Try increasing the timeout."),
  FORCE_26("Pipeline was stopped as the origin was retrieving data."),
  FORCE_27("Error parsing SOQL query {}"),
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}
