/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum ProcessingMode implements Label {
  RECORD("Record by Record"), BATCH("Record Batch");

  private String label;

  ProcessingMode(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
