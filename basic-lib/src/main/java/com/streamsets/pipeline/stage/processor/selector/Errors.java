/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.selector;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SELECTOR_00("Selector must have at least the default output stream"),
  SELECTOR_01("The number of conditions '{}' does not match the number of output streams '{}'"),
  SELECTOR_02("The Selector stage does not define the output stream '{}' associated with condition '{}'"),
  SELECTOR_03("Invalid condition '{}', {}"),
  SELECTOR_04("Invalid constants '{}', {}"),
  SELECTOR_05("Record did not match any condition"),
  SELECTOR_06("Record does not satisfy any condition, failing pipeline. Record '{}'"),
  SELECTOR_07("The last selection condition must be 'default'"),
  SELECTOR_08("Condition '{}' must be within '${...}'"),
  SELECTOR_09("Error evaluating record '{}' for '{}' condition, {}"),
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
