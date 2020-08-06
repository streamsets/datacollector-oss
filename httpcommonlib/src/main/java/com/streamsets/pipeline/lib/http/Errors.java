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
  HTTP_00("Cannot parse record. HTTP-Status: {} Reason: {}"),
  HTTP_01("Error fetching resource. HTTP-Status: {} Reason: {}"),
  HTTP_02("JSON parser found more than one record in chunk. Verify that the correct delimiter is configured."),
  HTTP_03("Error fetching resource. HTTP-Status: {} Reason: {}"),
  HTTP_04("The file '{}' does not exist or is inaccessible."),
  HTTP_05("Password is required for Key Store/Trust Store."),
  HTTP_06("Error evaluating expression: {}"),
  HTTP_07("Vault EL is only available when the resource scheme is https."),
  HTTP_08("HTTP-Status: {}. When using pagination, the results field must be a list but a {} was found."),
  HTTP_09("Chunked transfer encoding is not supported when using pagination."),
  HTTP_10("{} is not a supported data format when using pagination"),
  HTTP_11("HTTP-Status: {}. Record already contains field {}, cannot write response header."),
  HTTP_12("HTTP-Status: {}. Record does not contain result field path '{}'"),
  HTTP_13("Invalid Proxy URI. Reason : {}"),
  HTTP_14("Failing stage as per configuration. Status {}. Reason : {}"),
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
  HTTP_28("Exception in post processing or cleanup. HTTP-Status: {} Reason: {}"),
  HTTP_29("Can't resolve credential value for {}: {}"),
  HTTP_30("Can't resolve OAuth2 credentials: {}"),
  HTTP_31("Can't resolve OAuth1 credentials: {}"),
  HTTP_32("Error executing request. HTTP-Status: {} Reason: {}"),
  HTTP_33("Null authorization token - checked for '{}', '{}' and '{}'"),
  HTTP_34("HTTP-Status: {}. Received no entity in the HTTP message body."),
  HTTP_35("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
  HTTP_36("SPNEGO config file could not be created."),
  HTTP_37("\"{}\" action (for status {}) cannot be used in the HTTP Client Source"),

  // HTTP Target
  HTTP_40("Error sending resource. HTTP-Status: {} Reason: {}"),
  HTTP_41("Error sending resource. Reason: {}"),


  // WebSocket Target
  HTTP_50("Error sending resource. Reason: {}"),
  HTTP_51("Invalid Resource URI."),
  HTTP_52("Invalid Resource URI. Reason : {}"),
  HTTP_53("Invalid header: {}"),

  // HTTP Processor
  HTTP_61("HTTP-Status: {}. Cannot parse the field '{}' for record '{}': {}"),
  HTTP_62("Cannot parse the field '{}' as type {} is not supported"),
  HTTP_63("{} parsing the field '{}' as type {} for record '{}': {}"),
  HTTP_64("HTTP-Status: {}. IOException attempting to parse whole file field '{}' for record '{}': {}"),
  HTTP_65("HTTP-Status: {}. Input field '{}' does not exist in record '{}'"),
  HTTP_66("HTTP-Status: {}. Link field '{}' does not exist in record"),
  HTTP_67("HTTP-Status: {}. Not able to finish all retries because the batch was timed out. Please " +
      "increase the 'Batch Wait Time' or decrease the 'Base Backoff Interval'"),
  HTTP_68("No results for request: '{}'"),
  // HTTP Processor

  HTTP_100("Generating error record as per stage configuration: {}"),

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
