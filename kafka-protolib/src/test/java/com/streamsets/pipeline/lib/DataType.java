/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Label;

public enum DataType implements Label {
  LOG("Text"),
  JSON("JSON"),
  CSV("Delimited"),
  XML("XML"),

  ;

  private final String label;

  DataType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
