/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum PostProcessingOptions implements Label {
  NONE("None"),
  ARCHIVE("Archive"),
  DELETE("Delete"),
  ;


  private final String label;

  PostProcessingOptions(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
