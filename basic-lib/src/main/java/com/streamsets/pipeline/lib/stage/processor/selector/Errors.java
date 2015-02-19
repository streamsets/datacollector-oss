/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.selector;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SELECTOR_00("Selector must have at least 1 output stream"),
  SELECTOR_01("There are more output streams '{}' than stream-conditions '{}'"),
  SELECTOR_02("The Selector stage does not define the output stream '{}' associated with condition '{}'"),
  SELECTOR_03("Failed to validate the Selector condition '{}', {}"),
  SELECTOR_04("Failed to evaluate the Selector condition '{}', {}"),
  SELECTOR_05("Record did not match any condition"),
  SELECTOR_06("Record does not satisfy any condition, failing pipeline. Record '{}'"),
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
