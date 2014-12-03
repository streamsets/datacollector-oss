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
import com.streamsets.pipeline.container.ErrorMessage;
import com.streamsets.pipeline.container.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorSink {

  private final List<ErrorMessage> errors;
  private final Map<String, List<Record>> errorRecords;
  private int size;

  public ErrorSink() {
    errors = new ArrayList<>();
    errorRecords = new HashMap<>();
    size = 0;
  }

  public void addError(ErrorMessage errorMessage) {
    errors.add(errorMessage);
  }

  public void addRecord(String stageInstance, Record errorRecord) {
    List<Record> stageErrors = errorRecords.get(stageInstance);
    if (stageErrors == null) {
      stageErrors = new ArrayList<>();
      errorRecords.put(stageInstance, stageErrors);
    }
    stageErrors.add(errorRecord);
    size++;
  }

  public List<ErrorMessage> getErrors() {
    return errors;
  }

  public Map<String, List<Record>> getErrorRecords() {
    return errorRecords;
  }

  @SuppressWarnings("unchecked")
  public List<Record> getErrorRecords(String stageInstance) {
    List<Record> errors = errorRecords.get(stageInstance);
    return (errors != null) ? errors : Collections.EMPTY_LIST;
  }

  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return Utils.format("ErrorSink[reportingInstances='{}' size='{}']", errorRecords.keySet(), size());
  }

}
