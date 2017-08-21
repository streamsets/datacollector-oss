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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineStateJson {

  private final PipelineState pipelineState;
  private boolean ignoreMetrics = false;

  @JsonCreator
  public PipelineStateJson(
    @JsonProperty("pipelineId") String pipelineId,
    @JsonProperty("rev") String rev,
    @JsonProperty("user") String user,
    @JsonProperty("status") StatusJson statusJson,
    @JsonProperty("message") String message,
    @JsonProperty("timeStamp") long timeStamp,
    @JsonProperty("attributes") Map<String, Object> attributes,
    @JsonProperty("executionMode") ExecutionModeJson executionModeJson,
    @JsonProperty("metrics") String metrics,
    @JsonProperty("retryAttempt") int retryAttempt,
    @JsonProperty("nextRetryTimeStamp") long nextRetryTimeStamp,
    @JsonProperty("name") String name
  ) {
    if (pipelineId == null) {
      pipelineId = name;
    }
    pipelineState = new PipelineStateImpl(user, pipelineId, rev, BeanHelper.unwrapState(statusJson), message, timeStamp,
        attributes, BeanHelper.unwrapExecutionMode(executionModeJson), metrics, retryAttempt, nextRetryTimeStamp);
  }

  public PipelineStateJson(PipelineState pipelineState, boolean ignoreMetrics) {
    this.pipelineState = pipelineState;
    this.ignoreMetrics = ignoreMetrics;
  }

  public String getRev() {
    return pipelineState.getRev();
  }

  public String getUser() {
    return pipelineState.getUser();
  }

  public StatusJson getStatus() {
    return BeanHelper.wrapState(pipelineState.getStatus());
  }

  public String getMessage() {
    return pipelineState.getMessage();
  }

  public long getTimeStamp() {
    return pipelineState.getTimeStamp();
  }

  @Deprecated
  public String getName() {
    return pipelineState.getPipelineId();
  }

  public String getPipelineId() {
    return pipelineState.getPipelineId();
  }

  public Map<String, Object> getAttributes() {
    return pipelineState.getAttributes();
  }

  public ExecutionModeJson getExecutionMode() {
    return BeanHelper.wrapExecutionMode(pipelineState.getExecutionMode());
  }

  public String getMetrics() {
    if (!ignoreMetrics) {
      return pipelineState.getMetrics();
    } else {
      return null;
    }
  }

  public int getRetryAttempt() {
    return pipelineState.getRetryAttempt();
  }

  public long getNextRetryTimeStamp() {
    return pipelineState.getNextRetryTimeStamp();
  }

  @JsonIgnore
  public com.streamsets.datacollector.execution.PipelineState getPipelineState() {
    return pipelineState;
  }

}
