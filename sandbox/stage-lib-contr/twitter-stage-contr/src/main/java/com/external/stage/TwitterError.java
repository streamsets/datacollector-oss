/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.external.stage;

import com.streamsets.pipeline.api.ErrorCode;

public enum TwitterError implements ErrorCode {
  // We have an trailing whitespace for testing purposes
  TWITTER_001("There should be 1 input lane but there are '{}' "),
  TWITTER_002("There should be 1 output lane but there are '{}' ");

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