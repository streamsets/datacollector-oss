/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.security;

import com.streamsets.pipeline.api.ErrorCode;

public enum  Errors implements ErrorCode {
  SECURITY_001("PEM contains invalid base 64 data: {}"),
  SECURITY_002("Could not create a X.509 certificate out of the PEM: {}"),
  SECURITY_003("Could not add X.509 certificate to KeyStore: {}"),
  SECURITY_004("Could not save KeyStore to file: {}"),
  SECURITY_005("Could not create KeyStore file: {}"),
  SECURITY_006("Could not create a Private Key out of the PEM: {}"),
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
