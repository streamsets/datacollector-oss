/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  TAIL_00("Path '{}' does not exist"),
  TAIL_01("Path '{}' is not a directory"),
  TAIL_02("Could not create the directory scanner: {}"),

  TAIL_05("Could not deserialize offset '{}': {}"),
  TAIL_06("Could not scan the directory: {}"),
  TAIL_07("Could not open file '{}': {}"),
  TAIL_08("Error reading file '{}': {}"),

  TAIL_03("Invalid data format '{}'. Use one of the following formats: {}"),
  TAIL_04("Cannot parse record '{}': {}"),
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
