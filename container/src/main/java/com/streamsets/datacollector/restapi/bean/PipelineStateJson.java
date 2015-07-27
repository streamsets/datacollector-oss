/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
    @JsonProperty("executionMode") ExecutionModeJson executionModeJson) {
    pipelineState = new com.streamsets.datacollector.execution.manager.PipelineStateImpl(user, name, rev,
      BeanHelper.unwrapState(statusJson), message, timeStamp, attributes,
      BeanHelper.unwrapExecutionMode(executionModeJson));
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

  @JsonIgnore
  public com.streamsets.datacollector.execution.PipelineState getPipelineState() {
    return pipelineState;
  }

}