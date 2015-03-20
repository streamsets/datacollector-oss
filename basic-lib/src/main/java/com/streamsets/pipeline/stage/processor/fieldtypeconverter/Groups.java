/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
  TYPE_CONVERSION;

  @Override
  public String getLabel() {
    return "Conversions";
  }

}
