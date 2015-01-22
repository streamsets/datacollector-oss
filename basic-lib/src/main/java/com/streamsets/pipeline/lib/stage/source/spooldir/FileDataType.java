/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public enum FileDataType implements BaseEnumChooserValues.EnumWithLabel, ConfigGroups.Groups {
  LOG_FILES("Log Files"),
  JSON_FILES("JSON Files"),
  DELIMITED_FILES("Delimited Values Files (CVS, TSF)"),
  XML_FILES("XML Files"),
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
