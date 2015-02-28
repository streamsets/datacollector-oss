/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  XML_PARSER_00("Could advance reader '{}' to '{}' offset"),
  XML_PARSER_01("Could not obtain current reader position, {}"),
  XML_PARSER_02("XML object exceeded maximum length, readerId '{}' offset '{}' maximum length '{}'"),
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
