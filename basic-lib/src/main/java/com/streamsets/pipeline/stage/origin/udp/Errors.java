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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;


@GenerateResourceBundle
public enum Errors implements ErrorCode {
  UDP_00("Cannot bind to port {}: {}"),
  UDP_01("Unknown data format: {}"),
  UDP_02("No ports specified"),
  UDP_03("Port '{}' is invalid"),
  UDP_04("Charset '{}' is not supported"),
  UDP_05("collectd Types DB '{}' not found"),
  UDP_06("collectd Auth File '{}' not found"),
  UDP_07("Insufficient permissions to listen on privileged port {}"),
  UDP_08("Multithreaded UDP server is not available on your platform."),
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
