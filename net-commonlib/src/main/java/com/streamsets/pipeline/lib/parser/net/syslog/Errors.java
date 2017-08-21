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
package com.streamsets.pipeline.lib.parser.net.syslog;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;


@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SYSLOG_00("Corrupt message: '{}'"),
  SYSLOG_01("Corrupt message: invalid priority: {}: '{}'"),
  SYSLOG_02("Corrupt message: no data except priority: '{}'"),
  SYSLOG_03("Corrupt message: missing hostname: '{}'"),
  SYSLOG_04("Corrupt message: bad timestamp format: '{}'"),
  SYSLOG_05("Corrupt message: bad timestamp format: {}: '{}'"),
  SYSLOG_06("Corrupt message: bad timestamp format, no timezone: '{}'"),
  SYSLOG_07("Corrupt message: bad timestamp format, fractional portion: '{}'"),
  SYSLOG_08("Corrupt message: bad timestamp format, invalid timezone: '{}'"),
  SYSLOG_09("Not a valid RFC5424 timestamp: '{}'"),
  SYSLOG_10("Not a valid RFC3164 timestamp: '{}'"),
  SYSLOG_20("Error parsing Syslog message: '{}'"),
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
