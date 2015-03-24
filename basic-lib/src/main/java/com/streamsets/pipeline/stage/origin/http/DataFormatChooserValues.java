package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class DataFormatChooserValues extends BaseEnumChooserValues<DataFormat> {

  public DataFormatChooserValues() {
    super(DataFormat.JSON);
  }

}
