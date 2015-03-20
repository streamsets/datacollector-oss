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
  ELASTICSEARCH_00("Could not write record '{}': {}"),
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
