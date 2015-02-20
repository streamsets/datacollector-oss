/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class OnRecordErrorChooserValues extends BaseEnumChooserValues {

  public OnRecordErrorChooserValues() {
    super(OnRecordError.class);
  }

}
