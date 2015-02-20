/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class ConsumerDataFormatChooserValues extends BaseEnumChooserValues {

  public ConsumerDataFormatChooserValues() {
    super(DataFormat.TEXT, DataFormat.JSON, DataFormat.DELIMITED, DataFormat.XML, DataFormat.SDC_JSON);
  }

}
