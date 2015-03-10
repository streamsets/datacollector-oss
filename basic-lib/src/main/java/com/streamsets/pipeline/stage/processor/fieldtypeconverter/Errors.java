/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  CONVERTER_00("Failed to convert field '{}' with value '{}' to type '{}': {}"),
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
