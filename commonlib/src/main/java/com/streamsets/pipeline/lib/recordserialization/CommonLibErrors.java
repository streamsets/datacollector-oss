/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.ErrorCode;

public enum CommonLibErrors implements ErrorCode {

  COMMONLIB_0100("Unsupported field type '{}' with value '{}' encountered in record '{}'."),
  COMMONLIB_0101("Error converting record '{}' to String, reason: {}"),
  COMMONLIB_0102("Field Path to Column Name Mapping must be supplied to convert records into CSV format."),
  COMMONLIB_0103("Error converting record '{}' to String, reason: {}"),
  COMMONLIB_0104("Error converting record '{}' to 'SDC Record (JSON)' string, reason: {}"),

  ;
  private final String msg;

  CommonLibErrors(String msg) {
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
