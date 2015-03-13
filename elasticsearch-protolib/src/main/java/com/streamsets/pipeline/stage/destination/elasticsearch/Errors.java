/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.elasticsearch;

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
  ELASTICSEARCH_06("Cluster name cannot be empty"),
  ELASTICSEARCH_07("At least one URI must be provided"),
  ELASTICSEARCH_08("Could not connect to the cluster: {}"),
  ELASTICSEARCH_09("Invalid URI, it must be <HOSTNAME>:<PORT>: '{}'"),

  ELASTICSEARCH_10("Could not write record '{}': {}"),
  ELASTICSEARCH_11("Could not index record '{}': {}"),
  ELASTICSEARCH_12("Could not index '{}' records: {}"),

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
