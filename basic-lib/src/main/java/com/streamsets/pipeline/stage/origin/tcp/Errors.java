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
package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;


@GenerateResourceBundle
public enum Errors implements ErrorCode {
  TCP_00("Cannot bind to port {}: {}"),
  TCP_01("Unknown TCP mode: {}"),
  TCP_02("No ports specified"),
  TCP_03("Port '{}' is invalid"),
  TCP_04("Insufficient permissions to listen on privileged port {}"),
  TCP_05("Native transports for TCP server are not available on your platform."),
  TCP_06("Failing pipeline on error as per stage configuration: {}"),
  TCP_07("{} thrown in Netty channel pipeline: {}"),
  TCP_08("DataParserException thrown in Netty channel pipeline from DataFormatParserDecoder: {}"),
  TCP_09("No addresses available for TCP server to listen on"),
  TCP_10("Unrecognized charset: {}"),
  TCP_20("Unknown Syslog message framing mode: {}"),
  TCP_30("Invalid expression \"{}\" for record processed ack message: {}"),
  TCP_31("Invalid expression \"{}\" for batch completed ack message: {}"),
  TCP_35("Error evaluating {} expression: {}"),
  TCP_36("Record sent to next stage but got an exception when evaluating '{}' expression to send client response: {}"),
  TCP_40("Empty result (i.e. length of bytes was zero) after interpreting separator"),
  TCP_41("Separator string expression was not specified"),
  TCP_300("Avro IPC requires exactly one port"),
  TCP_301("Exception caught parsing data within Flume Avro IPC pipeline: {}"),
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
