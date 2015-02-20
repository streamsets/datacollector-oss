/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Label;

public enum DataFormat implements Label {
  TEXT("Text"),
  JSON("JSON"),
  DELIMITED("Delimited"),
  XML("XML"),
  SDC_JSON("SDC Record"),

  ;

  private final String label;

  DataFormat(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
