/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  REMOTE_01("Given URI is invalid {}"),
  REMOTE_02("Failed to process file '{}' at position '{}': {}"),

  REMOTE_03("Object in file '{}' at offset '{}' exceeds maximum length"),
  REMOTE_04("Failed to read data from file '{}' at position '{}' due to: {}"),
  REMOTE_05("Failed to read data due to: {}"),

  REMOTE_06("known_hosts file: {} does not exist or is not accessible"),
  REMOTE_07("Strict Host Checking is disabled and known_hosts file not specified"),
  REMOTE_08("Unable to download files from remote host: {}"),

  REMOTE_09("Poll Interval must be positive")
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
