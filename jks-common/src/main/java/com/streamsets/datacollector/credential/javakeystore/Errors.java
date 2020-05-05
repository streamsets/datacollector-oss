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
package com.streamsets.datacollector.credential.javakeystore;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  JKS_CRED_STORE_000("Missing configuration '{}'"),
  JKS_CRED_STORE_001("File '{}' does not exist"),
  JKS_CRED_STORE_002("Invalid KeyStore type '{}'"),
  JKS_CRED_STORE_003("Credential '{}' not found"),
  JKS_CRED_STORE_004("Credential '{}' could not be retrieved: {}"),
  JKS_CRED_STORE_005("Error initializing Credential Store. Reason : {}"),

  ;

  private final String message;

  Errors(String message) {
    this.message = message;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
