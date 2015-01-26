/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.splitter;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public enum OnNotEnoughSplits implements BaseEnumChooserValues.EnumWithLabel {
  CONTINUE("Continue"),
  DISCARD("Discard"),
  TO_ERROR("Send to Error"),
  STOP_PIPELINE("Stop Pipeline"),

  ;

  private final String label;

  OnNotEnoughSplits(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
