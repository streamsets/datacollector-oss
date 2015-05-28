package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  LOG_PARSER_00("Cannot advance file '{}' to offset '{}'"),
  LOG_PARSER_01("Error parsing log line '{}', reason : '{}'"),
  LOG_PARSER_02("Unsupported format {} encountered"),
  LOG_PARSER_03("Log line {} does not confirm to {}"),

  LOG_PARSER_04("Max data object length can be -1 or greater than 0"),
  LOG_PARSER_05("Custom Log Format field cannot be empty"),
  LOG_PARSER_06("Error parsing custom log format string {}, reason {}"),
  LOG_PARSER_07("Error parsing regex {}, reason {}"),
  LOG_PARSER_08("RegEx {} contains {} groups but the field Path to group mapping specifies group {}."),
  LOG_PARSER_09("Error parsing grok pattern {}, reason {}"),
  LOG_PARSER_10("Max stack trace lines field cannot be less than 0"),
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