/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MASK_00("Record '{}'"),
  MASK_01("{} is not a valid group number. The regular expression {} supports group numbers in range [1 - {}]"),
  MASK_02("Group numbers to show must be specified"),
  MASK_03("Regular Expression {} has no groups"),
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
