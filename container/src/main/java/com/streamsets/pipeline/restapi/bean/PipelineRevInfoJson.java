/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class PipelineRevInfoJson {

  private final com.streamsets.pipeline.store.PipelineRevInfo pipelineRevInfo;

  public PipelineRevInfoJson(com.streamsets.pipeline.store.PipelineRevInfo pipelineRevInfo) {
    this.pipelineRevInfo = pipelineRevInfo;
  }

  @JsonCreator
  public PipelineRevInfoJson(
    @JsonProperty("date") Date date,
    @JsonProperty("user") String user,
    @JsonProperty("rev") String rev,
    @JsonProperty("tag") String tag,
    @JsonProperty("description") String description,
    @JsonProperty("valid") boolean valid) {
    this.pipelineRevInfo = new com.streamsets.pipeline.store.PipelineRevInfo(date, user, rev, tag, description, valid);
  }

  public Date getDate() {
    return pipelineRevInfo.getDate();
  }

  public String getUser() {
    return pipelineRevInfo.getUser();
  }

  public String getRev() {
    return pipelineRevInfo.getRev();
  }

  public String getTag() {
    return pipelineRevInfo.getTag();
  }

  public String getDescription() {
    return pipelineRevInfo.getDescription();
  }

  public boolean isValid() {
    return pipelineRevInfo.isValid();
  }

  @JsonIgnore
  public com.streamsets.pipeline.store.PipelineRevInfo getPipelineRevInfo() {
    return pipelineRevInfo;
  }
}