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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ApiModel(description = "")
public class StageOutputJson   {

  private String instanceName = null;
  private Map<String, List<RecordJson>> output = new HashMap<String, List<RecordJson>>();
  private List<RecordJson> errorRecords = new ArrayList<RecordJson>();
  private List<ErrorMessageJson> stageErrors = new ArrayList<ErrorMessageJson>();
  private List<RecordJson> eventRecords = new ArrayList<RecordJson>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("instanceName")
  public String getInstanceName() {
    return instanceName;
  }
  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("output")
  public Map<String, List<RecordJson>> getOutput() {
    return output;
  }
  public void setOutput(Map<String, List<RecordJson>> output) {
    this.output = output;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorRecords")
  public List<RecordJson> getErrorRecords() {
    return errorRecords;
  }
  public void setErrorRecords(List<RecordJson> errorRecords) {
    this.errorRecords = errorRecords;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageErrors")
  public List<ErrorMessageJson> getStageErrors() {
    return stageErrors;
  }
  public void setStageErrors(List<ErrorMessageJson> stageErrors) {
    this.stageErrors = stageErrors;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("eventRecords")
  public List<RecordJson> getEventRecords() {
    return eventRecords;
  }
  public void setEventRecords(List<RecordJson> eventRecords) {
    this.eventRecords = eventRecords;
  }


  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class StageOutputJson {\n");

    sb.append("    instanceName: ").append(StringUtil.toIndentedString(instanceName)).append("\n");
    sb.append("    output: ").append(StringUtil.toIndentedString(output)).append("\n");
    sb.append("    errorRecords: ").append(StringUtil.toIndentedString(errorRecords)).append("\n");
    sb.append("    stageErrors: ").append(StringUtil.toIndentedString(stageErrors)).append("\n");
    sb.append("    eventRecords: ").append(StringUtil.toIndentedString(eventRecords)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
