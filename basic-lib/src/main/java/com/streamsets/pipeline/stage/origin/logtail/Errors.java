/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  TAIL_00("Log File '{}' does not exist"),
  TAIL_01("Insufficient permissions to read the log file '{}'"),
  TAIL_02("Invalid data format '{}', must be one of '{}'"),
  TAIL_03("Path '{}' is no a file"),
  TAIL_04("Could not parse record '{}', {}"),
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
