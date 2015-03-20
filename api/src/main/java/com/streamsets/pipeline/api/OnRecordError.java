/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.Label;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public enum OnRecordError implements Label {
  DISCARD("Discard"),
  TO_ERROR("Send to Error"),
  STOP_PIPELINE("Stop Pipeline"),

  ;

  private final String label;

  OnRecordError(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
