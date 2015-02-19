/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Label;

public enum ConfigGroups implements Label {
  FILES("Files"),
  POST_PROCESSING("Post Processing"),
  LOG_DATA("Text"),
  JSON_DATA("JSON"),
  DELIMITED_DATA("Delimited"),
  XML_DATA("XML"),
  ;

  private final String label;

  ConfigGroups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
