/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.CsvHeader;

public class HdfsCsvHeaderChooserValues  extends BaseEnumChooserValues<CsvHeader> {

  public HdfsCsvHeaderChooserValues() {
    super(CsvHeader.NO_HEADER, CsvHeader.WITH_HEADER);
  }

}
