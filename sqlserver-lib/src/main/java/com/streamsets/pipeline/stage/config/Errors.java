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
  SQLSERVER_00("Connection string must start with 'jdbc:sqlserver://'"),
  SQLSERVER_01("Connection must be encrypted"),
  SQLSERVER_02("Not allowed properties: {}"),
  SQLSERVER_03("Error processing server PEM certificate: {}"),
  SQLSERVER_04("The JDBC URL must be 'jdbc:sqlserver://<HostName>:1433;DatabaseName=<DATABASE>'"),
  SQLSERVER_05("Using SSH tunneling and host verification is not allowed, please specify an alternate hostname"),
  SQLSERVER_06("Could not find Runnable definition for Stage: {}"),
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
