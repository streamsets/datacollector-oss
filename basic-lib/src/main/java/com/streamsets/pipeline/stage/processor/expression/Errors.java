/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  EXPR_00("Invalid expression '{}': {}"),
  EXPR_01("Invalid constant '{}': {}"),
  EXPR_02("Record '{}' cannot set value in field '{}'"),
  EXPR_03("Cannot evaluate expression '{}' for record '{}': {}"),
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
