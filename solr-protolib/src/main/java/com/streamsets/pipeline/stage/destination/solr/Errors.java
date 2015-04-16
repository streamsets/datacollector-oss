/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SOLR_00("Solr URI cannot be empty"),
  SOLR_01("ZooKeeper Connection String cannot be empty"),
  SOLR_02("Fields value cannot be empty"),
  SOLR_03("Could not connect to the Solr instance: {}"),
  SOLR_04("Could not write record '{}': {}"),
  SOLR_05("Could not index '{}' records: {}"),

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
