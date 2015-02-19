/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.splitter;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SPLITTER_00("The number of splits fields must be greater than one"),
  SPLITTER_01("The record '{}' does not have the field-path '{}', cannot split"),
  SPLITTER_02("The record '{}' does not have enough splits"),
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
