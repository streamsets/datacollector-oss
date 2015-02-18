/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.dedup;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  DEDUP_00("Maximum record count must be greater than zero, it is '{}'"),
  DEDUP_01("Time window must be zero (disabled) or greater than zero, it is '{}'"),
  DEDUP_02("At least one field-path to hash must be specified"),
  DEDUP_03("The estimated required memory for '{}' records is '{}'MBs, current maximum heap is '{}'MBs, the " +
           "required memory must not exceed 1/3 of the maximum heap"),
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
