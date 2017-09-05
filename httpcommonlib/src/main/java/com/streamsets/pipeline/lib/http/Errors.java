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
package com.streamsets.pipeline.lib.http;

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
  HTTP_20("Content-Type header was present but was a {}, not a String"),
  HTTP_21("OAuth2 authentication failed. Please make sure the credentials are valid."),
  HTTP_22("OAuth2 authentication response does not contain access token"),
  HTTP_23("Token returned by authorization service does not have the authority to access the service. Please verify the credentials provided."),
  HTTP_24("Token URL was not found. Please verify that the URL: '{}' is correct, and the transfer encoding: '{}' is accepted"),
  HTTP_25("Unable to parse expression"), // Don't log expression as it could contain secure data
  HTTP_26("Algorithm '{}' is unavailable"),
  HTTP_27("Key is invalid: {}"),
  HTTP_28("Exception in post processing or cleanup: {}"),
  HTTP_29("Can't resolve credential value for {}: {}"),
  HTTP_30("Can't resolve OAuth2 credentials: {}"),
  HTTP_31("Can't resolve OAuth1 credentials: {}"),


  // HTTP Target
  HTTP_40("Error sending resource. Status: {} Reason: {}"),
  HTTP_41("Error sending resource. Reason: {}"),

  // WebSocket Target
  HTTP_50("Error sending resource. Reason: {}"),
  HTTP_51("Invalid Resource URI."),
  HTTP_52("Invalid Resource URI. Reason : {}"),

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

