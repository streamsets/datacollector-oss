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

import java.util.HashMap;
import java.util.Map;

@ApiModel(description = "")
public class PipelineStateJson   {

  private String user = null;
  private String name = null;
  private String pipelineId = null;
  private String rev = null;

  public enum StatusEnum {
    EDITED("EDITED"),
    STARTING("STARTING"),
    STARTING_ERROR("STARTING_ERROR"),
    START_ERROR("START_ERROR"),
    RUNNING("RUNNING"),
    RUNNING_ERROR("RUNNING_ERROR"),
    RUN_ERROR("RUN_ERROR"),
    FINISHING("FINISHING"),
    FINISHED("FINISHED"),
    RETRY("RETRY"),
    KILLED("KILLED"),
    STOPPING("STOPPING"),
    STOPPED("STOPPED"),
    STOPPING_ERROR("STOPPING_ERROR"),
    STOP_ERROR("STOP_ERROR"),
    DISCONNECTING("DISCONNECTING"),
    DISCONNECTED("DISCONNECTED"),
    CONNECTING("CONNECTING"),
    CONNECT_ERROR("CONNECT_ERROR"),
    DELETED("DELETED"),
    ;

    private final String value;

    StatusEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private StatusEnum status = null;
  private String message = null;
  private Long timeStamp = null;
  private Map<String, Object> attributes = new HashMap<String, Object>();

  public enum ExecutionModeEnum {
    STANDALONE("STANDALONE"),
    CLUSTER_BATCH("CLUSTER_BATCH"),
    CLUSTER_YARN_STREAMING("CLUSTER_YARN_STREAMING"),
    CLUSTER_MESOS_STREAMING("CLUSTER_MESOS_STREAMING"),
    SLAVE("SLAVE"),
    EDGE("EDGE"),
    EMR_BATCH("EMR_BATCH"),
    BATCH("BATCH"),
    STREAMING("STREAMING")
    ;

    private final String value;

    ExecutionModeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private ExecutionModeEnum executionMode = null;
  private String metrics = null;
  private Integer retryAttempt = null;
  private Long nextRetryTimeStamp = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("user")
  public String getUser() {
    return user;
  }
  public void setUser(String user) {
    this.user = user;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("name")
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("pipelineId")
  public String getPipelineId() {
    return pipelineId;
  }
  public void setPipelineId(String pipelineId) {
    this.pipelineId = pipelineId;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rev")
  public String getRev() {
    return rev;
  }
  public void setRev(String rev) {
    this.rev = rev;
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
  @JsonProperty("timeStamp")
  public Long getTimeStamp() {
    return timeStamp;
  }
  public void setTimeStamp(Long timeStamp) {
    this.timeStamp = timeStamp;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("attributes")
  public Map<String, Object> getAttributes() {
    return attributes;
  }
  public void setAttributes(Map<String, Object> attributes) {
    this.attributes = attributes;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("executionMode")
  public ExecutionModeEnum getExecutionMode() {
    return executionMode;
  }
  public void setExecutionMode(ExecutionModeEnum executionMode) {
    this.executionMode = executionMode;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("metrics")
  public String getMetrics() {
    return metrics;
  }
  public void setMetrics(String metrics) {
    this.metrics = metrics;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("retryAttempt")
  public Integer getRetryAttempt() {
    return retryAttempt;
  }
  public void setRetryAttempt(Integer retryAttempt) {
    this.retryAttempt = retryAttempt;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("nextRetryTimeStamp")
  public Long getNextRetryTimeStamp() {
    return nextRetryTimeStamp;
  }
  public void setNextRetryTimeStamp(Long nextRetryTimeStamp) {
    this.nextRetryTimeStamp = nextRetryTimeStamp;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class PipelineStateJson {\n");

    sb.append("    user: ").append(StringUtil.toIndentedString(user)).append("\n");
    sb.append("    name: ").append(StringUtil.toIndentedString(name)).append("\n");
    sb.append("    rev: ").append(StringUtil.toIndentedString(rev)).append("\n");
    sb.append("    status: ").append(StringUtil.toIndentedString(status)).append("\n");
    sb.append("    message: ").append(StringUtil.toIndentedString(message)).append("\n");
    sb.append("    timeStamp: ").append(StringUtil.toIndentedString(timeStamp)).append("\n");
    sb.append("    attributes: ").append(StringUtil.toIndentedString(attributes)).append("\n");
    sb.append("    executionMode: ").append(StringUtil.toIndentedString(executionMode)).append("\n");
    sb.append("    metrics: ").append(StringUtil.toIndentedString(metrics)).append("\n");
    sb.append("    retryAttempt: ").append(StringUtil.toIndentedString(retryAttempt)).append("\n");
    sb.append("    nextRetryTimeStamp: ").append(StringUtil.toIndentedString(nextRetryTimeStamp)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
