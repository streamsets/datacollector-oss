/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  HASH_00("Error creating message digest for {}, {}."),
  HASH_01("The record '{}' has the following issues - Fields '{}' do not exist, Fields '{}' have null value, " +
    "Fields '{}' are of type 'MAP' or 'LIST'.")
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
