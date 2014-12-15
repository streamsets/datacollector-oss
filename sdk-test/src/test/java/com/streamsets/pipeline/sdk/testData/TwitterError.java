/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.testData;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum TwitterError implements ErrorCode {
  INPUT_LANE_ERROR("There should be 1 input lane but there are '{}'"),
  OUTPUT_LANE_ERROR("There should be 1 output lane but there are '{}'");

  private final String msg;

  TwitterError(String msg) {
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
