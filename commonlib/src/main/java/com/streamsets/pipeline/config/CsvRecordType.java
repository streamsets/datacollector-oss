/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum CsvRecordType implements Label {
  LIST("List"),
  LIST_MAP("List-Map"),
  ;

  private final String label;

  CsvRecordType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
