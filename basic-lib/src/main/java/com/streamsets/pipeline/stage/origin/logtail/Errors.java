/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  TAIL_00("Log file '{}' does not exist"),
  TAIL_01("Insufficient permissions to read the log file '{}'"),
  TAIL_02("Invalid data format '{}'. Use one of the following formats: {}"),
  TAIL_03("Path '{}' is not a file"),
  TAIL_04("Cannot parse record '{}': {}"),
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
