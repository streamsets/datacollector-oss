/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.lib.kinesis;

import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class AWSRegionChooserValues extends BaseEnumChooserValues {

  public AWSRegionChooserValues() {
    super(Regions.class);
  }

}