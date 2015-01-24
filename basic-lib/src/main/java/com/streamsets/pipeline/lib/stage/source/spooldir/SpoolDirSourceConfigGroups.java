/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public enum SpoolDirSourceConfigGroups implements ConfigGroups.Groups {
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
