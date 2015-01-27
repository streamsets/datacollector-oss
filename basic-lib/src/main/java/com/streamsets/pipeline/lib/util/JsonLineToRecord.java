/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class JsonLineToRecord implements ToRecord {
  private ObjectMapper objectMapper;

  public JsonLineToRecord() {
    objectMapper = new ObjectMapper();

  }

  @Override
  public Record createRecord(Source.Context context, String sourceFile, long offset, String line, boolean truncated)
      throws ToRecordException {
    try {
      Object json = objectMapper.readValue(line, Object.class);
      Field field = JsonUtil.jsonToField(json);
      Record record = context.createRecord(Utils.format("{}::{}", sourceFile, offset));
      record.set(field);
      return record;
    } catch (Exception ex) {
      throw new ToRecordException(StageLibError.LIB_0005, line, ex.getMessage(), ex);
    }
  }

}
