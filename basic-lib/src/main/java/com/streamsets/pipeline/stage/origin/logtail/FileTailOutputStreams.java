/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum FileTailOutputStreams  implements Label {
  OUTPUT("Output"),
  METADATA("Metadata");

  private final String label;

  FileTailOutputStreams(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}