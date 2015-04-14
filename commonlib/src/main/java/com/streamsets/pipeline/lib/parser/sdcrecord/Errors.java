/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SDC_RECORD_PARSER_00("Could advance reader '{}' to '{}' offset"),
  SDC_RECORD_PARSER_01("Error reading header from stream, reason {}"),
  SDC_RECORD_PARSER_02("Unsupported Data Format number '{}' encountered in the stream."),

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
