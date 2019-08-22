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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  // Configuration errors
  CASSANDRA_00("No valid contact points provided."),
  CASSANDRA_01("Contact node value cannot be empty"),
  CASSANDRA_02("Table name must be fully qualified with a key space."),
  CASSANDRA_03("Failed to establish connection to Cassandra: {}"),
  CASSANDRA_04("Couldn't resolve hostname for contact node [{}]."),
  CASSANDRA_05("Could not connect to Cassandra cluster: {}"),
  CASSANDRA_06("Could not prepare record id'{}' due to: {}"),
  CASSANDRA_07("Could not insert batch due to: {}"),
  CASSANDRA_08("Invalid column mappings specified. Table doesn't have columns: {}"),
  CASSANDRA_09("Could not insert batch which included record: '{}': {}"),
  CASSANDRA_10("Requested Auth Provider '{}' not available. Please ensure you have the DSE driver jar installed."),
  CASSANDRA_11("Request timeout: Request didn't finish after {} milliseconds"),
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
