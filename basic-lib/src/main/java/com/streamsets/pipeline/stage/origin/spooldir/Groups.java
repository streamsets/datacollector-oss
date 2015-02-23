/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Label;

public enum Groups implements Label {
  FILES("Files"),
  POST_PROCESSING("Post Processing"),
  TEXT("Text"),
  JSON("JSON"),
  DELIMITED("Delimited"),
  XML("XML"),
  ;

  private final String label;

  Groups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
