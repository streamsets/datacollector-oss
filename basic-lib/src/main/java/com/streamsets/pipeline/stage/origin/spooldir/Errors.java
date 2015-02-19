/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SPOOLDIR_00("Could not archive file '{}' in error, {}"),
  SPOOLDIR_01("Error while processing file '{}' at position '{}', {}"),

  SPOOLDIR_02("Discarding Json Object, it exceeds maximum length '{}', file '{}', object starts at offset '{}'"),
  SPOOLDIR_03("LogDatProducer file='{}' offset='{}', {}"),
  SPOOLDIR_04("Discarding Xml Object, it exceeds maximum length '{}', file '{}', object starts at offset '{}'"),
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
