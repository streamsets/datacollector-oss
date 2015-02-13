/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import org.apache.commons.csv.CSVFormat;

public class TsvRecordToString extends CsvRecordToString {

  public TsvRecordToString() {
    super(CSVFormat.TDF);
  }

}
