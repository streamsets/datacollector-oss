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
import java.util.List;


@ApiModel(description = "")
public class PreviewOutputJson   {

  private String message = null;

public enum StatusEnum {
  VALIDATING("VALIDATING"), VALID("VALID"), INVALID("INVALID"), VALIDATION_ERROR("VALIDATION_ERROR"), STARTING("STARTING"), START_ERROR("START_ERROR"), RUNNING("RUNNING"), RUN_ERROR("RUN_ERROR"), FINISHING("FINISHING"), FINISHED("FINISHED"), CANCELLING("CANCELLING"), CANCELLED("CANCELLED"), TIMING_OUT("TIMING_OUT"), TIMED_OUT("TIMED_OUT");

  private String value;

  StatusEnum(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}

  private StatusEnum status = null;
  private IssuesJson issues = null;
  private List<List<StageOutputJson>> batchesOutput = new ArrayList<List<StageOutputJson>>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("message")
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("status")
  public StatusEnum getStatus() {
    return status;
  }
  public void setStatus(StatusEnum status) {
    this.status = status;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("issues")
  public IssuesJson getIssues() {
    return issues;
  }
  public void setIssues(IssuesJson issues) {
    this.issues = issues;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("batchesOutput")
  public List<List<StageOutputJson>> getBatchesOutput() {
    return batchesOutput;
  }
  public void setBatchesOutput(List<List<StageOutputJson>> batchesOutput) {
    this.batchesOutput = batchesOutput;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class PreviewOutputJson {\n");

    sb.append("    message: ").append(StringUtil.toIndentedString(message)).append("\n");
    sb.append("    status: ").append(StringUtil.toIndentedString(status)).append("\n");
    sb.append("    issues: ").append(StringUtil.toIndentedString(issues)).append("\n");
    sb.append("    batchesOutput: ").append(StringUtil.toIndentedString(batchesOutput)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
