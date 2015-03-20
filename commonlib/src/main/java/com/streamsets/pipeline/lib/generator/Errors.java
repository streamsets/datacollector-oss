/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  DATA_GENERATOR_00("Cannot open file '{}'"),

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
