/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.logtail;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public enum FileDataType implements BaseEnumChooserValues.EnumWithLabel, ConfigGroups.Groups {
  LOG_DATA("Log Data"),
  JSON_DATA("JSON Data"),
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
