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

package com.streamsets.pipeline.lib.parser.net.ssl;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.impl.ErrorMessage;

@GenerateResourceBundle
public enum SslConfigErrors implements ErrorCode {
  SSL_01("{} keystore file does not exist at {}"),
  SSL_02("Password file ({}) for {} keystore invalid: {}"),
  SSL_03("Error reading password file at {} for {} keystore: {}"),
  SSL_10("{} file does not exist at {}"),
  SSL_20("Error initializing {} keystore: {}"),
  SSL_30("Error reading certificate chain file at {}: {}"),
  ;

  SslConfigErrors(String msg) {
    this.msg = msg;
  }

  private final String msg;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }

}
