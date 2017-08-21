/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  IPC_DEST_00("host:port list cannot be empty"),
  IPC_DEST_01("host:port cannot be NULL"),
  IPC_DEST_02("Invalid 'hostname/IPv4:port' or '[IPv6]:port - '{}': {}"),
  IPC_DEST_03("Could not reach host '{}': {}"),
  IPC_DEST_04("Port '{}' out of range, valid range '1-65535'"),
  IPC_DEST_05("Invalid port number in '{}': {}"),
  IPC_DEST_06("There cannot be duplicate host:port values"),

  IPC_DEST_07("File does not exist"),
  IPC_DEST_08("Path is not a file"),
  IPC_DEST_09("File is not readable by the Data Collector"),
  IPC_DEST_10("Could not load trust certificates: {}"),
  IPC_DEST_11("Configuration value is empty"),

  IPC_DEST_12("HOST:PORT '{}' is not an SDC RPC end point"),

  IPC_DEST_15("Could not connect to any SDC RPC destination: {}"),

  IPC_DEST_20("Could not transmit: {}"),

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
