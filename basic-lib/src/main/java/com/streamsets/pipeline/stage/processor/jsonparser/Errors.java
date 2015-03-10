/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.ErrorCode;

/* LC: sorry Tucu, I changed the order of some of these. Change back if you want! */

public enum Errors implements ErrorCode {
  JSONP_00("JSON field '{}' does not exist in record '{}'. Cannot parse the field."),
  JSONP_01("JSON field '{}' in record '{}' is set to NULL. Cannot parse the field."),
  JSONP_02("Cannot pass the parsed JSON to field '{}' for record '{}'. Field does not exist."),
  JSONP_03("Cannot parse the JSON field '{}' for record '{}': {}"),
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
