/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  GEOIP_00("Database file {} does not exist"),
  GEOIP_01("Error reading database file '{}': '{}'"),
  GEOIP_02("Address {} not found: '{}'"),
  GEOIP_03("Unknown geolocation occurred: '{}'"),
  GEOIP_04("At least one field is required"),
  GEOIP_05("Supplied database does not support: '{}'"),
  GEOIP_06("String IP addresses must be dot delimited [0-9].[0-9].[0-9].[0-9] or an integer: '{}'"),
  GEOIP_07("Unknown error occurred during initialization: '{}'")
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
