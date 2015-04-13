/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class OutputRecordFormatChooserValues extends BaseEnumChooserValues<DataFormat> {

  public OutputRecordFormatChooserValues() {
    super(DataFormat.SDC_JSON, DataFormat.JSON);
  }

}
