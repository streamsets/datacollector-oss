/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.recordstolocalfilesystem;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
    /* LC: can we delete '{}' from the first one - not necessary, I don't think? */
  RECORDFS_00("Max file size '{}' must be zero or greater"),
  RECORDFS_01("Directory '{}' does not exist"),
  RECORDFS_02("Path '{}' is not a directory"),
    /* LC: can we delete "it is '{}'" from the following? */
  RECORDFS_03("Rotation interval '{}' must be greater than zero, it is '{}'"),
  RECORDFS_04("Rotation interval '{}' is not a valid expression"),
  RECORDFS_05("Could not write record to file '{}': {}"),
  RECORDFS_06("Could not rotate file '{}': {}"),
  RECORDFS_07("Could not rotate file '{}': {}"),
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
