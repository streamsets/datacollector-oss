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
  TAIL_01("At least one directory must be specified"),
  TAIL_02("Could not initialize multi-directory reader: {}"),
  TAIL_03("Invalid data format '{}'"),

  TAIL_10("Could not deserialize offset: {}"),
  TAIL_11("Error reading file '{}': {}"),
  TAIL_12("Cannot parse record '{}': {}"),
  TAIL_13("Could not serialize offset: {}"),
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
