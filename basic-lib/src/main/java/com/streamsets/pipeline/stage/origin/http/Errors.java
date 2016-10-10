/*
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HTTP_00("Cannot parse record: {}"),
  HTTP_01("Error fetching resource. Status: {} Reason: {}"),
  HTTP_02("JSON parser found more than one record in chunk. Verify that the correct delimiter is configured."),
  HTTP_03("Error fetching resource. Reason: {}"),
  HTTP_04("The file '{}' does not exist or is inaccessible."),
  HTTP_05("Password is required for Key Store/Trust Store."),
  HTTP_06("Error evaluating expression: {}"),
  HTTP_07("Vault EL is only available when the resource scheme is https."),
  HTTP_08("When using pagination, the results field must be a list but a {} was found"),
  HTTP_09("Chunked transfer encoding is not supported when using pagination."),
  HTTP_10("{} is not a supported data format when using pagination"),
  HTTP_11("Record already contains field {}, cannot write response header."),
  HTTP_12("Record does not contain result field path '{}'"),
  HTTP_13("Invalid Proxy URI. Reason : {}"),
  HTTP_14("Failing stage as per configuration for status {}. Reason : {}"),
  HTTP_15("When using backoff, base interval must be greater than 0"),
  HTTP_16("Actions can only be configured for non-OK statuses (i.e. not in the [200,300) range)"),
  HTTP_17("A particular status code can only be mapped to one action.  Code {} was mapped more than once."),
  HTTP_18("Failing stage as per configuration for read timeout"),
  HTTP_19("Failing stage because number of request retries exceeded configured maximum of {}"),
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

