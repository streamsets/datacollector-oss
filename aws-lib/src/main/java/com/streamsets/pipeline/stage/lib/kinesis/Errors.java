/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.lib.kinesis;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  KINESIS_00("Failed to put record: {}. Cause: {}"),
  KINESIS_01("Specified stream name is not available. Ensure you've specified the correct AWS Region. Cause: {}"),
  KINESIS_02("Unrecognized partition strategy: {}"),
  KINESIS_03("Failed to parse incoming Kinesis record w/ sequence number: {}"),
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
