/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

import java.util.concurrent.TimeUnit;

public class TimeUnitChooserValues extends BaseEnumChooserValues {

  public TimeUnitChooserValues() {
    super(TimeUnit.class);
  }

}
