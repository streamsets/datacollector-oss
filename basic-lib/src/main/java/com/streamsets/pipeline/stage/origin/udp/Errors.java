/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;


@GenerateResourceBundle
public enum Errors implements ErrorCode {
  UDP_00("Cannot bind to port {}: {}"),
  UDP_01("Cannot parse record '{}': {}"),
  UDP_02("No ports specified"),
  UDP_03("Port '{}' is invalid"),
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
