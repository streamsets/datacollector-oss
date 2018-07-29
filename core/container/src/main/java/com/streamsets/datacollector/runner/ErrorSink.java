/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ErrorSink implements ReportErrorDelegate {

  private final Map<String, List<ErrorMessage>> stageErrors;
  private final Map<String, List<Record>> errorRecords;
  private int size;
  private int totalErrorRecords;
  private int totalErrorMessages;

  public ErrorSink() {
    stageErrors = new LinkedHashMap<>();
    errorRecords = new LinkedHashMap<>();
    size = 0;
    totalErrorMessages = 0;
    totalErrorRecords = 0;
  }

  // for SDK
  public void clear() {
    stageErrors.clear();
    errorRecords.clear();
  }

  @Override
  public void reportError(String stage, ErrorMessage errorMessage) {
    addError(stageErrors, stage, errorMessage);
    totalErrorMessages++;
  }

  public void addRecord(String stage, Record errorRecord) {
    addError(errorRecords, stage, errorRecord);
    totalErrorRecords++;
  }

  public Map<String, List<ErrorMessage>> getStageErrors() {
    return stageErrors;
  }

  public Map<String, List<Record>> getErrorRecords() {
    return errorRecords;
  }

  private <T> void addError(Map<String, List<T>> map, String stage, T error) {
    List<T> errors = map.computeIfAbsent(stage, k -> new ArrayList<>());
    errors.add(error);
    size++;
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> getErrors(Map<String, List<T>> map, String stage) {
    List<T> errors = map.get(stage);
    return (errors != null) ? errors : Collections.emptyList();
  }

  public List<Record> getErrorRecords(String stage) {
    return getErrors(errorRecords, stage);
  }

  public List<ErrorMessage> getStageErrors(String stage) {
    return getErrors(stageErrors, stage);
  }

  public int size() {
    return size;
  }

  @Override
  public String toString() {
    Set<String> stages = new HashSet<>(errorRecords.keySet());
    stages.addAll(stageErrors.keySet());
    return Utils.format("ErrorSink[reportingInstances='{}' size='{}']", stages, size());
  }

  public int getTotalErrorRecords() {
    return totalErrorRecords;
  }

  public int getTotalErrorMessages() {
    return totalErrorMessages;
  }

}
