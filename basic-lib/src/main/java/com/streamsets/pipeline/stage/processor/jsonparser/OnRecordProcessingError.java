/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.Label;

public enum OnRecordProcessingError implements Label {
  DISCARD("Discard"),
  TO_ERROR("Send to Error"),
  STOP_PIPELINE("Stop Pipeline"),

  ;

  private final String label;

  OnRecordProcessingError(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
