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
package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum AerospikeErrors implements ErrorCode {
  AEROSPIKE_01("Config is required"),
  AEROSPIKE_02("Invalid URI '{}'"),
  AEROSPIKE_03("Unable to connect to Aerospike on '{}'"),
  AEROSPIKE_04("Put operation was not successful '{}'"),
  AEROSPIKE_05("There has to be at least one bin to store"),
  AEROSPIKE_06("'{}' can not be parsed: '{}'"),
  AEROSPIKE_07("'{}' can not be evaluated: '{}'"),;

  private final String msg;

  AerospikeErrors(String msg) {
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
