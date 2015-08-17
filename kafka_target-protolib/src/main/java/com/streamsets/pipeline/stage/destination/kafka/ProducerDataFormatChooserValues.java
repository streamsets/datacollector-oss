/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class ProducerDataFormatChooserValues extends BaseEnumChooserValues<DataFormat> {

  public ProducerDataFormatChooserValues() {
    super(DataFormat.SDC_JSON, DataFormat.TEXT, DataFormat.JSON, DataFormat.DELIMITED, DataFormat.AVRO, DataFormat.BINARY);
  }

}
