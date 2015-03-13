package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  LOG_PARSER_00("Cannot advance file '{}' to offset '{}'"),
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