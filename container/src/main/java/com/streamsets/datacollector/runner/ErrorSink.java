/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ErrorSink {

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

  public void addError(String stage, ErrorMessage errorMessage) {
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
    List<T> errors = map.get(stage);
    if (errors == null) {
      errors = new ArrayList<>();
      map.put(stage, errors);
    }
    errors.add(error);
    size++;
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> getErrors(Map<String, List<T>> map, String stage) {
    List<T> errors = map.get(stage);
    return (errors != null) ? errors : Collections.EMPTY_LIST;
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
