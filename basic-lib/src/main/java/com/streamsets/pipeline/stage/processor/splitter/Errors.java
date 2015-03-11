/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SPLITTER_00("Define at least two fields to split"),
  SPLITTER_01("Field cannot split. The record '{}' does not include the field '{}'."),
  SPLITTER_02("The record '{}' does not have enough splits"),
  SPLITTER_03("Field Path at index '{}' cannot be empty")
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
