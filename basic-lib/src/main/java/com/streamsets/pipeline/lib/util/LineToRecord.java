/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class LineToRecord {
  private static final String LINE = "line";
  private static final String TRUNCATED = "truncated";

  private boolean setTruncated;

  public LineToRecord(boolean setTruncated) {
    this.setTruncated = setTruncated;
  }

  public Record createRecord(Source.Context context, String sourceFile, long offset, String line, boolean truncated) {
    Record record = context.createRecord(Utils.format("file={} offset={}", sourceFile, offset));
    Map<String, Field> map = new LinkedHashMap<>();
    map.put(LINE, Field.create(line));
    if (setTruncated) {
      map.put(TRUNCATED, Field.create(truncated));
    }
    record.set(Field.create(map));
    return record;
  }

}
