/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class StageOutputJson {

  private final com.streamsets.pipeline.runner.StageOutput stageOutput;

  @JsonCreator
  public StageOutputJson(
    @JsonProperty("instanceName") String instanceName,
    @JsonProperty("output") Map<String, List<RecordJson>> output,
    @JsonProperty("errorRecords") List<RecordJson> errorRecordJsons,
    @JsonProperty("stageErrors") List<ErrorMessageJson> stageErrors) {
    this.stageOutput = new com.streamsets.pipeline.runner.StageOutput(instanceName, BeanHelper.unwrapRecordsMap(output),
      BeanHelper.unwrapRecords(errorRecordJsons), BeanHelper.unwrapErrorMessages(stageErrors));
  }

  public StageOutputJson(com.streamsets.pipeline.runner.StageOutput stageOutput) {
    this.stageOutput = stageOutput;
  }

  public String getInstanceName() {
    return stageOutput.getInstanceName();
  }

  public Map<String, List<RecordJson>> getOutput() {
    return BeanHelper.wrapRecordsMap(stageOutput.getOutput());
  }

  public List<RecordJson> getErrorRecords() {
    return BeanHelper.wrapRecords(stageOutput.getErrorRecords());
  }

  public List<ErrorMessageJson> getStageErrors() {
    return BeanHelper.wrapErrorMessages(stageOutput.getStageErrors());
  }

  @JsonIgnore
  public com.streamsets.pipeline.runner.StageOutput getStageOutput() {
    return stageOutput;
  }
}