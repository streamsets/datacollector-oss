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
package com.streamsets.pipeline.stage.config;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  POSTGRES_00("Connection string must start with 'jdbc:postgresql://'"),
  POSTGRES_01("Connection must be encrypted"),
  POSTGRES_02("Not allowed properties: {}"),
  POSTGRES_03("Error saving PEM: {}"),
  POSTGRES_04("Invalid certificate PEM, missing BEGIN/END markers"),
  POSTGRES_05("The JDBC URL must be 'jdbc:postgresql://<HOST>[:<PORT>][/<DB>]...'"),
  POSTGRES_06(
      "Using SSH tunneling and the Verify Full mode for SSL/TLS encryption is not allowed since it is not possible to" +
          " verify the host name."),
  ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}
