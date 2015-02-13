/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

public class DataCollectorRecordToString implements RecordToString {
  private final Stage.Context context;

  public DataCollectorRecordToString(Stage.Context context) {
    this.context = context;
  }

  @Override
  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap) {
  }

  @Override
  public String toString(Record record) throws StageException {
    StringWriter writer = new StringWriter();
    try (JsonRecordWriter rw = ((ContextExtensions)context).createJsonRecordWriter(writer)) {
      rw.write(record);
    } catch (IOException ex) {
      throw new StageException(CommonLibErrors.COMMONLIB_0104, record.getHeader().getSourceId(), ex.getMessage(), ex);
    }
    return writer.toString();
  }

}
