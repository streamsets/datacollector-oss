/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum OriginalFieldAction implements Label {
  REMOVE("Remove"),
  KEEP("Keep"),
  ;

  private final String label;

  OriginalFieldAction(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
