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
package com.streamsets.pipeline.lib.parser.net.netflow;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;


@GenerateResourceBundle
public enum Errors implements ErrorCode {
  NETFLOW_00("Invalid version: '{}'"),
  NETFLOW_01("Corrupt packet: {}"),
  NETFLOW_02("Unexpected error: {}"),
  NETFLOW_10("Unrecognized flowset ID of {} (less than 256, but not 0 [template] or 1 [options])"),
  NETFLOW_11("Message field referenced flowset template ID {}, but that was not seen in a template record"),
  NETFLOW_12("Expected single byte field with type ID {}, but it was actually {} bytes"),
  NETFLOW_13("Error parsing IPV6 address ({}): {}"),
  NETFLOW_14("Error parsing IPV4 address from bytes {}: {}"),
  NETFLOW_15("Max template cache size must be a positive number, or -1 to indicate unlimited"),
  NETFLOW_16("Template cache timeout (ms) be a positive number, or -1 to indicate unlimited"),
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
