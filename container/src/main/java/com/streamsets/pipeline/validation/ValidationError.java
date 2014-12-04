/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;

public enum ValidationError implements ErrorCode {

  VALIDATION_0001("The pipeline is empty"),
  VALIDATION_0002("Stages are not fully wired, cannot reach the following stages '{}'"),
  VALIDATION_0003("The first stage must be a Source"),
  VALIDATION_0004("This stage cannot be a Source"),
  VALIDATION_0005("Instance name already defined"),
  VALIDATION_0006("Stage definition does not exist, library '{}' name '{}' version '{}'"),
  VALIDATION_0007("Configuration value is required"),
  VALIDATION_0008("Invalid configuration"),
  VALIDATION_0009("Configuration should be a '{}'"),
  VALIDATION_0010("Output lanes '{}' already defined by stage instance '{}'"),
  VALIDATION_0011("Instance has open lanes '{}'"),
  VALIDATION_0012("{} cannot have input lanes '{}'"),
  VALIDATION_0013("{} cannot have output lanes '{}'"),
  VALIDATION_0014("{} must have input lanes"),
  VALIDATION_0015("{} must have output lanes"),
  VALIDATION_0016("Invalid instance name, names can only contain the following characters '{}'"),
  VALIDATION_0017("Invalid input lane names '{}', lanes can only contain the following characters '{}'"),
  VALIDATION_0018("Invalid output lane names '{}', lanes can only contain the following characters '{}'"),

  ;

  private final String msg;

  ValidationError(String msg) {
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
