/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Label;

public enum OnParseError implements Label {
  ERROR("ERROR"),
  IGNORE("IGNORE"),
  INCLUDE_AS_STACK_TRACE("INCLUDE AS STACK TRACE")
  ;

  private final String label;

  OnParseError(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
