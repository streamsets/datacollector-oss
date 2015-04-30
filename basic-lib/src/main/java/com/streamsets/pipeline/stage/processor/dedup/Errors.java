/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.dedup;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

/* LC: For the first two - do we need to tell them what the value is? Can't we just tell them what it needs to be?
* Can we say:
* Maximum record count must be greater than zero
* Time to compare must be a positive integer or zero to opt out of a time comparison */

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  DEDUP_00("Maximum record count must be greater than zero, it is '{}'"),
  DEDUP_01("Time window must be zero (disabled) or greater than zero, it is '{}'"),
  DEDUP_02("Specify at least one field for comparison"),
  DEDUP_03("The estimated required memory for '{}' records is '{}' MB. The current maximum heap is '{}' MB. The " +
           "required memory must not exceed the maximum heap."),
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
