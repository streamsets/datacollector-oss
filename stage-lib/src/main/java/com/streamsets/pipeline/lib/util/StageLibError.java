/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;

public enum StageLibError implements ErrorCode {

  // LogTailSource
  LIB_0001("Insufficient permissions to read the log file '{}'"),

  // AbstractSpoolDirSource
  LIB_0100("Could not archive file '{}' in error, {}"),
  LIB_0101("Error while processing file '{}' at position '{}', {}"),

  // JsonSpoolDirSource
  LIB_0200("Discarding Json Object '{}', it exceeds maximum length '{}', file '{}', object starts at offset '{}'"),

  ;

  private final String msg;

  StageLibError(String msg) {
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
