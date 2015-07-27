/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

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

  public StageOutput(String instanceName, Map<String, List<Record>> output, List<Record> errorRecords,
        List<ErrorMessage> stageErrors) {
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
