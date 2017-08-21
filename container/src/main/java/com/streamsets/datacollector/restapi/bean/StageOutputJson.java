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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.runner.StageOutput;

import java.util.List;
import java.util.Map;

public class StageOutputJson {

  private final com.streamsets.datacollector.runner.StageOutput stageOutput;

  @JsonCreator
  public StageOutputJson(
    @JsonProperty("instanceName") String instanceName,
    @JsonProperty("output") Map<String, List<RecordJson>> output,
    @JsonProperty("errorRecords") List<RecordJson> errorRecordJsons,
    @JsonProperty("stageErrors") List<ErrorMessageJson> stageErrors,
    @JsonProperty("eventRecords") List<RecordJson> eventRecords
    ) {
    this.stageOutput = new StageOutput(instanceName,
      BeanHelper.unwrapRecordsMap(output),
      BeanHelper.unwrapRecords(errorRecordJsons),
      BeanHelper.unwrapErrorMessages(stageErrors),
      BeanHelper.unwrapRecords(eventRecords)
    );
  }

  public StageOutputJson(com.streamsets.datacollector.runner.StageOutput stageOutput) {
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

  public List<RecordJson> getEventRecords() {
    return BeanHelper.wrapRecords(stageOutput.getEventRecords());
  }

  @JsonIgnore
  public com.streamsets.datacollector.runner.StageOutput getStageOutput() {
    return stageOutput;
  }
}
