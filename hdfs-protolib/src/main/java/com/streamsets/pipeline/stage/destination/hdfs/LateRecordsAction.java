/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum LateRecordsAction implements Label {
  SEND_TO_ERROR("Send to error"),
  SEND_TO_LATE_RECORDS_FILE("Send to late records file"),

  ;
  private final String label;
  LateRecordsAction(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
