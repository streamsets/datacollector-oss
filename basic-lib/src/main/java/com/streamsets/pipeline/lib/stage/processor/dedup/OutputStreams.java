/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.dedup;

import com.streamsets.pipeline.api.Label;

public enum OutputStreams implements Label {
  UNIQUE("Unique Records"),
  DUPLICATE("Duplicate Records"),
  ;

  private final String label;

  OutputStreams(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
