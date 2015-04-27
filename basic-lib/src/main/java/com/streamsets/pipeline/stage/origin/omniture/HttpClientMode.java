/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.omniture;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum HttpClientMode implements Label {
  POLLING("Polling"),
  ;

  private final String label;

  HttpClientMode(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
