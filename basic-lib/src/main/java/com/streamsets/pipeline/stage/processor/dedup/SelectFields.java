/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.dedup;

import com.streamsets.pipeline.api.Label;

public enum SelectFields implements Label {
  ALL_FIELDS("All Fields"),
  SPECIFIED_FIELDS("Specified Fields"),
  ;

  private final String label;

  SelectFields(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
