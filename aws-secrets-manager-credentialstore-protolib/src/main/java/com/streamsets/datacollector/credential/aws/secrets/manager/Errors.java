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
package com.streamsets.datacollector.credential.aws.secrets.manager;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  AWS_SECRETS_MANAGER_CRED_STORE_00("Missing configuration '{}'"),
  AWS_SECRETS_MANAGER_CRED_STORE_01("Unable to connect to AWS Secrets Manager. Verify your configuration is correct. " +
      "Message: {}"),
  AWS_SECRETS_MANAGER_CRED_STORE_02("Credential '{}' could not be retrieved"),
  AWS_SECRETS_MANAGER_CRED_STORE_03("Credential '{}' could not be retrieved'. Message: {}"),
  AWS_SECRETS_MANAGER_CRED_STORE_04("Key '{}' could not be be retrieved from Credential '{}'"),
  AWS_SECRETS_MANAGER_CRED_STORE_05("Security method must be set to 'accessKeys' or 'instanceProfile'"),
  AWS_SECRETS_MANAGER_CRED_STORE_06(
      "If 'accessKeys' authentication method is selected, both 'accessKey' and 'secretKey' must be set"),
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
