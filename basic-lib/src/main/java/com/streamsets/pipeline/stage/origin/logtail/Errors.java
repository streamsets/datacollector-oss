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
  TAIL_04("The same file path (and pattern) cannot be specified more than once: '{}'"),
  TAIL_05("Archive directory cannot be empty"),
  TAIL_06("Archive directory does not exist"),
  TAIL_07("Archive directory path is not a directory"),
  TAIL_08("The configuration for '{}' requires the '{}' token in the '{}' file name"),
  TAIL_09("The configuration for '{}' has an invalid regular expression in the '{}' pattern: {}"),
  TAIL_15("The file path '{}' must have the '{}' token in it"),

  TAIL_10("Could not deserialize offset: {}"),
  TAIL_11("Error reading file '{}': {}"),
  TAIL_12("Cannot parse record '{}': {}"),
  TAIL_13("Could not serialize offset: {}"),
  TAIL_14("Could not get file start/end events: {}"),
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
