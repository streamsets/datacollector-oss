/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmerger;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  FIELD_MERGER_00("Record '{}' does not contain fields '{}'"),
  FIELD_MERGER_01("Fields '{}' cannot be overwritten for record '{}'"),
  FIELD_MERGER_02("Cannot merge field of type {} with field of type {}"),
  FIELD_MERGER_03("Field merger only supports LIST and MAP types"),
  FIELD_MERGER_04("Parent field for target does not exist")
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