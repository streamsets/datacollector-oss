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
package com.streamsets.pipeline.stage.executor.databricks;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  DATABRICKS_01("Job with ID: '{}' does not exist"),
  DATABRICKS_02("Base URL is invalid"),
  DATABRICKS_03("Error while requesting job listing"),
  DATABRICKS_04("Incorrect job type for job ID: '{}'. Expected: '{}', found: '{}'"),
  DATABRICKS_05("Invalid credentials"),
  DATABRICKS_06("Running Job with ID: '{}' failed with error: '{}'"),
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
