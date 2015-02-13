/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.selector;

import com.streamsets.pipeline.api.Label;

public enum OnNoPredicateMatch implements Label {
  DROP_RECORD("Discard"),
  RECORD_TO_ERROR("Send to Error"),
  FAIL_PIPELINE("Stop Pipeline"),
  ;

  private final String label;
  OnNoPredicateMatch(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }


}
