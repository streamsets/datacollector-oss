/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.Label;

public enum FileDataType implements Label {
  LOG_DATA("Text"),
  JSON_DATA("JSON"),
  DELIMITED_DATA("Delimited"),
  XML_DATA("XML"),
  SDC_RECORDS("SDC Record"),

  ;

  private final String label;

  FileDataType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
