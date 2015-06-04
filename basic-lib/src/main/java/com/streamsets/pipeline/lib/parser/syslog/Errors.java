/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.syslog;

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