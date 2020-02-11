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
package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.ErrorCode;

public enum QueryExecErrors implements ErrorCode {
  QUERY_EXECUTOR_001("Failed to execute query '{}': {}"),
  QUERY_EXECUTOR_002("Can't open connection: {}"),
  QUERY_EXECUTOR_003("Maximum Pool Size must be the same value as Minimum Idle Connections "
      + " for parallel operation."),
  QUERY_EXECUTOR_004("Error executing statement {} "),
  QUERY_EXECUTOR_005("Connection Error {}"),
  QUERY_EXECUTOR_006("Exception while waiting for completion {}"),
  QUERY_EXECUTOR_007("Queries cannot be blank"),
  ;

  private final String msg;

  QueryExecErrors(String msg) {
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
