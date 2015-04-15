/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum MaskType implements Label {
  FIXED_LENGTH("Fixed length"),
  VARIABLE_LENGTH("Variable length"),
  CUSTOM("Custom"),
  REGEX("Regular Expression")

  ;

  private final String label;

  MaskType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }


}
