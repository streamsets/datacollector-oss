/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.datacollector.credential.azure.keyvault;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  KEY_VAULT_CRED_STORE_00("Missing configuration '{}'"),
  KEY_VAULT_CRED_STORE_01("Store '{}' failed to retrieve credential '{}', will wait '{}' ms"),
  KEY_VAULT_CRED_STORE_02("Credential '{}' could not be retrieved"),
  KEY_VAULT_CRED_STORE_03("Client cannot be created due exception: '{}'"),
  KEY_VAULT_CRED_STORE_04("Client authorization exception, wrong client id/key: '{}'"),
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