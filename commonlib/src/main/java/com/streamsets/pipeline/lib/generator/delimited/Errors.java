/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.delimited;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  DELIMITED_GENERATOR_00("Record '{}' root field should be a 'LIST' but it is a '{}'"),
  DELIMITED_GENERATOR_01("Record '{}' column '{}' field should be a 'MAP' but it is a '{}'"),
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
