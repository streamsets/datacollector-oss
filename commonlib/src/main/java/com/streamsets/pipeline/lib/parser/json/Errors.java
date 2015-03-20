/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  JSON_PARSER_00("Cannot advance reader '{}' to offset '{}'"),
  JSON_PARSER_01("Cannot parse JSON: readerId '{}', offset '{}', unsupported type '{}'"),
  JSON_PARSER_02("JSON object exceeded maximum length: readerId '{}', offset '{}', maximum length '{}'"),
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
