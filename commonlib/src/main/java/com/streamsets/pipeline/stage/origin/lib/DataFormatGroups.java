/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum DataFormatGroups implements Label {
  TEXT("Text"),
  JSON("JSON"),
  DELIMITED("Delimited"),
  XML("XML"),
  LOG("Log"),
  AVRO("Avro"),

  ;

  private final String label;

  DataFormatGroups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
