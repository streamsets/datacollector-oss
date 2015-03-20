/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum OnStagePreConditionFailure implements Label {
  CONTINUE("Continue"),
  TO_ERROR("Send to Error"),

  ;

  private final String label;

  OnStagePreConditionFailure(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
