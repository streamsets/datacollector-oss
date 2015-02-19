/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.Label;

public enum FilterOperation implements Label {
  KEEP("Keep Listed Fields"),
  REMOVE("Remove Listed Fields");

  private String label;

  private FilterOperation(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
