/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum CommonError implements ErrorCode {

  //Kafka source and Target messages
  CMN_0100("Unsupported field type '{}' with value '{}' encountered in record '{}'"),
  CMN_0101("Error converting record '{}' to string: {}"),
  CMN_0102("Field Path to Column Name mapping must be configured to convert records to CSV."),
  CMN_0103("Error converting record '{}' to string: {}"),
  CMN_0104("Error evaluating expression {}: {}"),
  CMN_0105("Error parsing expression {}: {}"),

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
