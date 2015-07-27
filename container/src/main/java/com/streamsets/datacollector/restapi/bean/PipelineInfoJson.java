/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Date;
import java.util.UUID;

public class PipelineInfoJson {
  private final com.streamsets.datacollector.store.PipelineInfo pipelineInfo;

  @JsonCreator
  public PipelineInfoJson(
    @JsonProperty("name") String name,
    @JsonProperty("description") String description,
    @JsonProperty("created") Date created,
    @JsonProperty("lastModified") Date lastModified,
    @JsonProperty("creator") String creator,
    @JsonProperty("lastModifier") String lastModifier,
    @JsonProperty("lastRev") String lastRev,
    @JsonProperty("uuid") UUID uuid,
    @JsonProperty("valid") boolean valid) {
    this.pipelineInfo = new com.streamsets.datacollector.store.PipelineInfo(name, description, created, lastModified,
      creator, lastModifier, lastRev, uuid, valid);
  }

  public PipelineInfoJson(com.streamsets.datacollector.store.PipelineInfo pipelineInfo) {
    Utils.checkNotNull(pipelineInfo, "pipelineInfo");
    this.pipelineInfo = pipelineInfo;
  }

  public String getName() {
    return pipelineInfo.getName();
  }

  public String getDescription() {
    return pipelineInfo.getDescription();
  }

  public Date getCreated() {
    return pipelineInfo.getCreated();
  }

  public Date getLastModified() {
    return pipelineInfo.getLastModified();
  }

  public String getCreator() {
    return pipelineInfo.getCreator();
  }

  public String getLastModifier() {
    return pipelineInfo.getLastModifier();
  }

  public String getLastRev() {
    return pipelineInfo.getLastRev();
  }

  public UUID getUuid() {
    return pipelineInfo.getUuid();
  }

  public boolean isValid() {
    return pipelineInfo.isValid();
  }

  @JsonIgnore
  public com.streamsets.datacollector.store.PipelineInfo getPipelineInfo() {
    return pipelineInfo;
  }
}