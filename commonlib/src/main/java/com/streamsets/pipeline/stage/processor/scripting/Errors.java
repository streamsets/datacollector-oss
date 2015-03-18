/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SCRIPTING_00("Scripting engine '{}' not found"),
  SCRIPTING_01("Could not load scripting engine '{}': {}"),
  SCRIPTING_02("Script cannot be empty"),

  SCRIPTING_05("Script error while processing record: {}"),
  SCRIPTING_06("Script error while processing batch: {}"),

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
