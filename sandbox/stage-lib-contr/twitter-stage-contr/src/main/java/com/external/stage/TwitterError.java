package com.external.stage;

import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.api.StageErrorDef;

@StageErrorDef
public enum TwitterError implements ErrorId {
  // We have an trailing whitespace for testing purposes
  INPUT_LANE_ERROR("There should be 1 input lane but there are '{}' "),
  OUTPUT_LANE_ERROR("There should be 1 output lane but there are '{}' ");

  private String msg;

  TwitterError(String msg) {
    this.msg = msg;
  }

  @Override
  public String getMessageTemplate() {
    return msg;
  }
}