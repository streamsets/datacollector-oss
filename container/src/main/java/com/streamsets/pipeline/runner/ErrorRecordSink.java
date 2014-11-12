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

import java.util.HashMap;
import java.util.Map;

public class ErrorRecordSink {
  private final static ErrorRecords EMPTY = new ErrorRecords();
  private final Map<String, ErrorRecords> errorRecords;
  private int size;

  public ErrorRecordSink() {
    errorRecords = new HashMap<String, ErrorRecords>();
    size = 0;
  }

  public void addRecord(String stageInstance, ErrorRecord errorRecord) {
    ErrorRecords stageErrors = errorRecords.get(stageInstance);
    if (stageErrors == null) {
      stageErrors = new ErrorRecords();
      errorRecords.put(stageInstance, stageErrors);
    }
    stageErrors.addErrorRecord(errorRecord);
    size++;
  }

  public Map<String, ErrorRecords> getErrorRecords() {
    return errorRecords;
  }

  public ErrorRecords getErrorRecords(String stageInstance) {
    ErrorRecords errors = errorRecords.get(stageInstance);
    return (errors != null) ? errors : EMPTY;
  }

  public int size() {
    return size;
  }

}
