/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.Label;

public enum SpoolDirSourceConfigGroups implements Label {
  FILES("Files"),
  POST_PROCESSING("Post Processing"),
  ;

  private final String label;

  SpoolDirSourceConfigGroups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
