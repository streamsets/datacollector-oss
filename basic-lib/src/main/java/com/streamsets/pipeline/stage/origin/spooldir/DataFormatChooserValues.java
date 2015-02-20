/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class DataFormatChooserValues extends BaseEnumChooserValues {

  public DataFormatChooserValues() {
    super(DataFormat.TEXT, DataFormat.JSON, DataFormat.DELIMITED, DataFormat.XML, DataFormat.SDC_JSON);
  }

}
