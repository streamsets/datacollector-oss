/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.execution.PipelineState;

import java.util.Map;

public class PipelineStateJson {

  private final PipelineState pipelineState;

  @JsonCreator
  public PipelineStateJson(
    @JsonProperty("user") String user,
    @JsonProperty("name") String name,
    @JsonProperty("rev") String rev,
    @JsonProperty("status") StatusJson statusJson,
    @JsonProperty("message") String message,
    @JsonProperty("timeStamp") long timeStamp,
    @JsonProperty("attributes") Map<String, Object> attributes,
    @JsonProperty("executionMode") ExecutionModeJson executionModeJson,
    @JsonProperty("metrics") String metrics,
    @JsonProperty("retryAttempt") int retryAttempt,
    @JsonProperty("nextRetryTimeStamp") long nextRetryTimeStamp) {
    pipelineState = new com.streamsets.datacollector.execution.manager.PipelineStateImpl(user, name, rev,
      BeanHelper.unwrapState(statusJson), message, timeStamp, attributes,
      BeanHelper.unwrapExecutionMode(executionModeJson), metrics, retryAttempt, nextRetryTimeStamp);
  }

  public PipelineStateJson(com.streamsets.datacollector.execution.PipelineState pipelineState) {
    this.pipelineState = pipelineState;
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

  public String getName() {
    return pipelineState.getName();
  }

  public Map<String, Object> getAttributes() {
    return pipelineState.getAttributes();
  }

  public ExecutionModeJson getExecutionMode() {
    return BeanHelper.wrapExecutionMode(pipelineState.getExecutionMode());
  }

  public String getMetrics() {
    return pipelineState.getMetrics();
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