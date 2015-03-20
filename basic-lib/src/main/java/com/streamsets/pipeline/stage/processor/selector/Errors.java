/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.selector;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SELECTOR_00("The Stream Selector must include a default output stream"),
  SELECTOR_01("The number of conditions '{}' does not match the number of output streams '{}'"),
  SELECTOR_02("The Stream Selector does not define the output stream '{}' associated with condition '{}'"),
  SELECTOR_03("Invalid condition '{}': {}"),
  SELECTOR_04("Invalid constant '{}': {}"),
  SELECTOR_05("Record '{}' does not match any condition"),
  SELECTOR_06("Record '{}' does not satisfy any condition. Failing the pipeline."),
  SELECTOR_07("The last condition must be 'default'"),
  SELECTOR_08("Define the condition '{}' using the following syntax: ${<condition>}"),
  SELECTOR_09("Error evaluating record '{}' for '{}' condition: {}"),
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
