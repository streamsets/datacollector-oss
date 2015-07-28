/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  CASSANDRA_03("Failed to establish connection to Cassandra"),
  CASSANDRA_04("Couldn't resolve hostname for contact node [{}]."),
  CASSANDRA_05("Could not connect to Cassandra cluster: {}"),
  CASSANDRA_06("Could not prepare record '{}': {}"),
  CASSANDRA_07("Could not insert batch."),
  CASSANDRA_08("Invalid column mappings specified. Table doesn't have columns: {}"),
  CASSANDRA_09("Could not insert batch which included record: '{}': {}")
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
