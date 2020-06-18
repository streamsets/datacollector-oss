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
package com.streamsets.pipeline.lib.tls;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum TlsConfigErrors implements ErrorCode {
  TLS_01("{}store file does not exist at {}"),
  TLS_02("{} is a not valid path for {}store because it contains an empty space."),
  TLS_05("Key store and/or trust store file must be specified."),
  TLS_20("Error attempting to create {}store instance: {}"),
  TLS_21("Error attempting to load {}store at {}: {}"),
  TLS_22("Error attempting to load key manager algorithm {}: {}"),
  TLS_23("Error attempting to initialize key store with key manager: {}"),
  TLS_50("Error attempting to load trust manager algorithm {}: {}"),
  TLS_51("Error attempting to initialize trust store with key manager: {}"),
  TLS_60("Certificate Chain must contain at least one certificate"),
  ;

  TlsConfigErrors(String msg) {
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
