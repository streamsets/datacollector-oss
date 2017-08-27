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
  REMOTE_07("Strict Host Checking is enabled and known_hosts file not specified"),
  REMOTE_08("Unable to download files from remote host: {} with given credentials. " +
      "Please verify if the host is reachable, and the credentials are valid."),

  REMOTE_09("Poll Interval must be positive"),
  REMOTE_10("Private Key file: {} does not exist or is not accessible"),
  REMOTE_11("Private Key authentication is supported only with SFTP"),
  REMOTE_12("Strict Host Checking is supported only with SFTP"),
  REMOTE_13("File Pattern cannot be empty"),
  REMOTE_14("Invalid GLOB file pattern '{}': {}"),
  REMOTE_15("URI: '{}' is invalid. Must begin with 'ftp://' or 'sftp://'"),
  REMOTE_16("Initial file '{}' is invalid: {}"),
  REMOTE_17("Can't resolve credential: {}"),
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
