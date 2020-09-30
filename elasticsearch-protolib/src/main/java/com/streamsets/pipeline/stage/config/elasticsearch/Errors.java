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
package com.streamsets.pipeline.stage.config.elasticsearch;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  ELASTICSEARCH_00("Could not parse the index template expression: {}"),
  ELASTICSEARCH_01("Could not evaluate the index template expression: {}"),
  ELASTICSEARCH_02("Could not parse the type template expression: {}"),
  ELASTICSEARCH_03("Could not evaluate the type template expression: {}"),
  ELASTICSEARCH_04("Could not parse the docId template expression: {}"),
  ELASTICSEARCH_05("Could not evaluate the docId template expression: {}"),
  ELASTICSEARCH_06("HTTP URI cannot be empty"),
  ELASTICSEARCH_07("Invalid URI, it must be <HOSTNAME>:<PORT>: '{}'"),
  ELASTICSEARCH_08("Port value out of range: '{}'"),
  ELASTICSEARCH_09("Unable to authenticate user [{}] for REST request [{}]"),
  ELASTICSEARCH_10("TrustStore path is provided but not TrustStore password"),
  ELASTICSEARCH_11("TrustStore path is set but points to a non-existing file: {}"),
  ELASTICSEARCH_12("Could not configure SSL ({})"),
  ELASTICSEARCH_13("Operation not supported: {}"),
  ELASTICSEARCH_14("Unknown action for unsupported operation: {}"),
  ELASTICSEARCH_15("Could not write record '{}' ({})"),
  ELASTICSEARCH_16("Could not index record '{}': {}"),
  ELASTICSEARCH_17("Could not index '{}' records: {}"),
  ELASTICSEARCH_18("Could not evaluate the time driver expression: {}"),
  ELASTICSEARCH_19("Document ID expression must be provided to use {} operation"),
  ELASTICSEARCH_20("User cannot be empty"),
  // Origin
  ELASTICSEARCH_21("Could not find _scroll_id field in response to query."),
  ELASTICSEARCH_22("Failed to fetch batch ({})"),
  ELASTICSEARCH_23("Cursor expired, please Reset Origin and restart the pipeline."),
  ELASTICSEARCH_24("Offset field '{}' not found in parsed record."),
  ELASTICSEARCH_25("Incremental mode requires the query to contain ${OFFSET} in at least one field"),
  ELASTICSEARCH_26("Changing the parallelism from '{}' to '{}' slices requires resetting the origin as it recomputes shards."),
  ELASTICSEARCH_27("Could not parse the parent ID template expression: {}"),
  ELASTICSEARCH_28("Could not evaluate the parent ID template expression: {}"),
  ELASTICSEARCH_29("Could not parse the routing template expression: {}"),
  ELASTICSEARCH_30("Could not evaluate the routing template expression: {}"),
  ELASTICSEARCH_31("Can't resolve password for TrustStore: {}"),
  ELASTICSEARCH_32("Can't resolve user: {}"),
  ELASTICSEARCH_33("Endpoint cannot be empty"),
  ELASTICSEARCH_34("Invalid Json format [{}] ({})"),
  ELASTICSEARCH_35("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
  ELASTICSEARCH_36("Could not parse the additional properties expression: {}"),
  ELASTICSEARCH_37("Could not evaluate the additional properties expression: {}"),
  ELASTICSEARCH_38("Can't resolve password: {}"),
  ELASTICSEARCH_39("Invalid user name or password"),
  ELASTICSEARCH_40("User name and password cannot be null"),
  ELASTICSEARCH_41("Invalid query: {}"),
  ELASTICSEARCH_42("Could not get response body"),
  ELASTICSEARCH_43("Could not connect to the server(s) [{}] ({})"),
  ELASTICSEARCH_44("Invalid REST request [{}]"),
  ELASTICSEARCH_45("Unexpected server response: {} {} {}"),
  ELASTICSEARCH_46("User [{}] is not authorized to send REST request [{}]"),
  ELASTICSEARCH_47("Unable to authenticate for REST request [{}]"),
  ELASTICSEARCH_48("User is not authorized to send REST request [{}]"),
  ELASTICSEARCH_49("Unexpected JSON response [{}]"),
  ELASTICSEARCH_50("Invalid path [{}] ({})"),
  ELASTICSEARCH_51("No PathEscape implementation found"),
  ELASTICSEARCH_52("There should be only one implementation of PathEscape");

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
