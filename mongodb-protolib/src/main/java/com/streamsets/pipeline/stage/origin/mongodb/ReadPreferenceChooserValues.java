/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class ReadPreferenceChooserValues extends BaseEnumChooserValues {

  public ReadPreferenceChooserValues() {
    super(ReadPreferenceLabel.class);
  }
}
