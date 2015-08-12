/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  IPC_DEST_00("host:port list cannot be empty"),
  IPC_DEST_01("host:port cannot be NULL"),
  IPC_DEST_02("Invalid '<host>:<port>': {}"),
  IPC_DEST_03("Could not reach host '{}': {}"),
  IPC_DEST_04("Port '{}' out of range, valid range '1-65535'"),
  IPC_DEST_05("Invalid port number in '{}': {}"),
  IPC_DEST_06("There cannot be duplicate host:port values"),

  IPC_DEST_07("File does not exist"),
  IPC_DEST_08("Path is not a file"),
  IPC_DEST_09("File is not readable by the Data Collector"),
  IPC_DEST_10("Could not load trust certificates: {}"),
  IPC_DEST_11("Configuration value is empty"),

  IPC_DEST_15("Could not connect to any IPC destination: {}"),

  IPC_DEST_20("Could not transmit: {}"),
  IPC_DEST_21("Invalid OnErrorRecord setting '{}'"),

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
