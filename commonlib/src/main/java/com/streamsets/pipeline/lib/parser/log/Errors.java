package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  LOG_PARSER_00("Cannot advance file '{}' to offset '{}'"),
  LOG_PARSER_01("Error parsing log line '{}', reason : '{}'"),
  LOG_PARSER_02("Unsupported format {} encountered"),
  LOG_PARSER_03("Log line {} does not confirm to {}"),
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