/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;

public enum CommonError implements ErrorCode {

  //Kafka source and Target messages
  CMN_0100("Unsupported field type '{}' with value '{}' encountered in record '{}'."),
  CMN_0101("Error converting record '{}' to String, reason: {}"),
  CMN_0102("Field Path to Column Name Mapping must be supplied to convert records into CSV format."),
  CMN_0103("Error converting record '{}' to String, reason: {}"),
  CMN_0104("Error evaluating expression {}, reason: {}"),
  CMN_0105("Error parsing expression {}, reason: {}"),

  ;
  private final String msg;

  CommonError(String msg) {
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
