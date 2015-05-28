/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  LOGP_00("Field '{}' does not exist in record '{}'. Cannot parse the field."),
  LOGP_01("Field '{}' in record '{}' is set to NULL. Cannot parse the field."),
  LOGP_02("Cannot pass the parsed Log line to field '{}' for record '{}'. Field does not exist."),
  LOGP_03("Cannot parse the Long line field '{}' for record '{}': {}"),
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
