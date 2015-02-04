/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.CommonError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogRecordToString implements RecordToString {

  private static final Logger LOG = LoggerFactory.getLogger(LogRecordToString.class);

  @Override
  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap) {

  }

  @Override
  public String toString(Record record) throws StageException {
    try {
      return record.get().getValueAsString();
    } catch (Exception e) {
      LOG.error(CommonError.CMN_0103.getMessage(), record.getHeader().getSourceId(), e.getMessage());
      throw new StageException(CommonError.CMN_0103, record.getHeader().getSourceId(), e.getMessage(), e);
    }
  }
}
