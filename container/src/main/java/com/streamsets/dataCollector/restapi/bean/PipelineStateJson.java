/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.pipeline.restapi.bean.BeanHelper;

import java.util.Map;

public class PipelineStateJson {

  private final PipelineState pipelineState;

  @JsonCreator
  public PipelineStateJson(
    @JsonProperty("name") String name,
    @JsonProperty("rev") String rev,
    @JsonProperty("user") String user,
    @JsonProperty("state") StatusJson stateJson,
    @JsonProperty("message") String message,
    @JsonProperty("timeStamp") long timeStamp,
    @JsonProperty("attributes") Map<String, Object> attributes) {
    pipelineState = new com.streamsets.dataCollector.execution.manager.PipelineStateImpl(name, rev, user, BeanHelper.unwrapState(stateJson),
      message, timeStamp, attributes);
  }

  public PipelineStateJson(com.streamsets.dataCollector.execution.PipelineState pipelineState) {
    this.pipelineState = pipelineState;
  }

  public String getRev() {
    return pipelineState.getRev();
  }

  public String getUser() {
    return pipelineState.getUser();
  }

  public StatusJson getState() {
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

  @JsonIgnore
  public com.streamsets.dataCollector.execution.PipelineState getPipelineState() {
    return pipelineState;
  }

}