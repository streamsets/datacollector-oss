/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  JSONP_00("Record '{}' does not have the field-path '{}', cannot parse"),
  JSONP_01("Record '{}' has the field-path '{}' set to NULL, cannot parse"),
  JSONP_02("Record '{}' cannot set the parsed JSON  at field-path='{}', field-path does not exist"),
  JSONP_03("Record '{}' could not JSON parse field '{}': {}"),
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
