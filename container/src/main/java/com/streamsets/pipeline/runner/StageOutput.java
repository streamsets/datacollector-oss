/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.List;
import java.util.Map;

public class StageOutput {
  private final String instanceName;
  private final Map<String, List<Record>> output;
  private final List<Record> errorRecords;
  private final List<ErrorMessage> stageErrors;

  @SuppressWarnings("unchecked")
  public StageOutput(String instanceName, Map<String, List<Record>> output, ErrorSink errorSink) {
    this(instanceName, (Map) output, (List) errorSink.getErrorRecords(instanceName),
         errorSink.getStageErrors().get(instanceName));
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  //TODO fix generics typing here, now using RecordImpl to trick jackson into deserializing records
  public StageOutput(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("output") Map<String, List<RecordImpl>> output,
      @JsonProperty("errorRecords") List<RecordImpl> errorRecords,
        @JsonProperty("stageErrors") List<ErrorMessage> stageErrors) {
    this.instanceName = instanceName;
    this.output = (Map) output;
    this.errorRecords = (List) errorRecords;
    this.stageErrors = stageErrors;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public Map<String, List<Record>> getOutput() {
    return output;
  }

  public List<Record> getErrorRecords() {
    return errorRecords;
  }

  public List<ErrorMessage> getStageErrors() {
    return stageErrors;
  }

  @Override
  public String toString() {
    return Utils.format("StageOutput[instance='{}' lanes='{}']", instanceName, output.keySet());
  }

}
