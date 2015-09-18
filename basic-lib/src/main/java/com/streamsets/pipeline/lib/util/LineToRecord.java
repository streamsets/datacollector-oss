/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;

import java.util.LinkedHashMap;
import java.util.Map;

public class LineToRecord implements ToRecord {
  private static final String LINE = "line";
  private static final String TRUNCATED = "truncated";

  private boolean setTruncated;

  public LineToRecord(boolean setTruncated) {
    this.setTruncated = setTruncated;
  }

  @Override
  public Record createRecord(Source.Context context, String sourceFile, long offset, String line, boolean truncated) {
    Record record = context.createRecord(sourceFile + "::" + offset);
    Map<String, Field> map = new LinkedHashMap<>();
    map.put(LINE, Field.create(line));
    if (setTruncated) {
      map.put(TRUNCATED, Field.create(truncated));
    }
    record.set(Field.create(map));
    return record;
  }

}
