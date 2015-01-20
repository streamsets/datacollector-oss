/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.Map;

public class LogRecordToString implements RecordToString {
  @Override
  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap) {

  }

  @Override
  public String toString(Record record) throws StageException {
    return record.get().getValueAsString();
  }
}
