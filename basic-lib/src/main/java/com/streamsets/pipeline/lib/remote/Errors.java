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
package com.streamsets.pipeline.lib.remote;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  REMOTE_01("Given URI is invalid {}"),
  REMOTE_02("URI: '{}' is invalid. Must begin with 'ftp://' or 'sftp://'"),
  REMOTE_03("known_hosts file: {} does not exist or is not accessible"),
  REMOTE_04("Strict Host Checking is enabled and known_hosts file not specified"),
  REMOTE_05("Private Key file: {} does not exist or is not accessible"),
  REMOTE_06("Private Key authentication is supported only with SFTP"),
  REMOTE_07("Strict Host Checking is supported only with SFTP"),
  REMOTE_08("Can't resolve credential: {}"),
  REMOTE_09("Error accessing remote directory: {}"),
  REMOTE_10("Unable to load Private Key: {}"),
  REMOTE_11("Unable to connect to remote host '{}' with given credentials. " +
      "Please verify if the host is reachable, and the credentials are valid. Message: {}"),
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
