/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kinesis;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class InputRecordFormatChooserValues extends BaseEnumChooserValues<DataFormat> {
  public InputRecordFormatChooserValues() {
    super(DataFormat.SDC_JSON, DataFormat.JSON);
  }
}
