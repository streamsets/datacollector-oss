/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
  TCP_01("Unknown data format: {}"),
  TCP_02("No ports specified"),
  TCP_03("Port '{}' is invalid"),
  TCP_04("Charset '{}' is not supported"),
  TCP_05("collectd Types DB '{}' not found"),
  TCP_06("collectd Auth File '{}' not found"),
  TCP_07("Insufficient permissions to listen on privileged port {}"),
  TCP_08("Multithreaded TCP server is not available on your platform."),
  TCP_09("Failing pipeline on error as per stage configuration: {}"),
  TCP_10("{} caught in Netty channel pipeline: {}"),
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
