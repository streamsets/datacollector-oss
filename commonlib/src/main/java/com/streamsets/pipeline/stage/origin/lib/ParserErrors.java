/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum ParserErrors implements ErrorCode {
  // Configuration errors
  PARSER_01("Unsupported charset '{}'"),
  PARSER_02("Invalid XML element name '{}'"),
  PARSER_03("Cannot parse record from message '{}': {}"),
  PARSER_04("Max data object length cannot be less than 1"),
  PARSER_05("Unsupported data format '{}'"),
  PARSER_06("Messages with XML data cannot have multiple XML documents in a single message"),
  PARSER_07("Avro Schema must be specified"),
  ;
  private final String msg;

  ParserErrors(String msg) {
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