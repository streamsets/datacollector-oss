/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class PipelineStateJson {

  private final com.streamsets.pipeline.prodmanager.PipelineState pipelineState;

  @JsonCreator
  public PipelineStateJson(
    @JsonProperty("name") String name,
    @JsonProperty("rev") String rev,
    @JsonProperty("state") StateJson stateJson,
    @JsonProperty("message") String message,
    @JsonProperty("lastStatusChange") long lastStatusChange,
    @JsonProperty("metrics") String metrics,
    @JsonProperty("attributes") Map<String, Object> attributes) {
    pipelineState = new com.streamsets.pipeline.prodmanager.PipelineState(name, rev, BeanHelper.unwrapState(stateJson),
      message, lastStatusChange, metrics, attributes);
  }

  public PipelineStateJson(com.streamsets.pipeline.prodmanager.PipelineState pipelineState) {
    this.pipelineState = pipelineState;
  }

  public String getRev() {
    return pipelineState.getRev();
  }

  public StateJson getState() {
    return BeanHelper.wrapState(pipelineState.getState());
  }

  public String getMessage() {
    return pipelineState.getMessage();
  }

  public long getLastStatusChange() {
    return pipelineState.getLastStatusChange();
  }

  public String getName() {
    return pipelineState.getName();
  }

  public String getMetrics() {
    return pipelineState.getMetrics();
  }

  public Map<String, Object> getAttributes() {
    return pipelineState.getAttributes();
  }

  @JsonIgnore
  public com.streamsets.pipeline.prodmanager.PipelineState getPipelineState() {
    return pipelineState;
  }

}