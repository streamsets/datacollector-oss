/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorRecordSink {
  private final Map<String, List<ErrorRecord>> errorRecords;
  private int size;

  public ErrorRecordSink() {
    errorRecords = new HashMap<String, List<ErrorRecord>>();
    size = 0;
  }

  public void addRecord(String stageInstance, Record record, ErrorRecord.ERROR error, Object... args) {
    List<ErrorRecord> stageErrors = errorRecords.get(stageInstance);
    if (stageErrors == null) {
      stageErrors = new ArrayList<ErrorRecord>();
      errorRecords.put(stageInstance, stageErrors);
    }
    stageErrors.add(new ErrorRecord(record, error, args));
    size++;
  }

  public Map<String, List<ErrorRecord>> getErrorRecords() {
    return errorRecords;
  }

  public List<ErrorRecord> getErrorRecords(String stageInstance) {
    return errorRecords.get(stageInstance);
  }

  public int size() {
    return size;
  }

}
