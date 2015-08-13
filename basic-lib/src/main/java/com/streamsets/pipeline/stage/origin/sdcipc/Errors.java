/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  IPC_ORIG_00("Port out of range"),
  IPC_ORIG_01("Port not available: {}"),

  IPC_ORIG_07("File does not exist"),
  IPC_ORIG_08("Path is not a file"),
  IPC_ORIG_09("File is not readable by the Data Collector"),
  IPC_ORIG_10("Could not load key store: {}"),
  IPC_ORIG_11("Configuration value is empty"),

  IPC_ORIG_20("Could not start IPC server: {}"),
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
