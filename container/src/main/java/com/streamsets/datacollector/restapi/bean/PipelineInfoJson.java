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
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineInfoJson {
  private final PipelineInfo pipelineInfo;

  @JsonCreator
  public PipelineInfoJson(
    @JsonProperty("pipelineId") String pipelineId,
    @JsonProperty("title") String title,
    @JsonProperty("description") String description,
    @JsonProperty("created") Date created,
    @JsonProperty("lastModified") Date lastModified,
    @JsonProperty("creator") String creator,
    @JsonProperty("lastModifier") String lastModifier,
    @JsonProperty("lastRev") String lastRev,
    @JsonProperty("uuid") UUID uuid,
    @JsonProperty("valid") boolean valid,
    @JsonProperty("metadata") Map<String, Object> metadata,
    @JsonProperty("name") String name,
    @JsonProperty("sdcVersion") String sdcVersion,
    @JsonProperty("sdcId") String sdcId
  ) {
    if (pipelineId == null) {
      pipelineId = name;
    }
    this.pipelineInfo = new PipelineInfo(pipelineId, title, description, created, lastModified,
      creator, lastModifier, lastRev, uuid, valid, metadata, sdcVersion, sdcId);
  }

  public PipelineInfoJson(PipelineInfo pipelineInfo) {
    Utils.checkNotNull(pipelineInfo, "pipelineInfo");
    this.pipelineInfo = pipelineInfo;
  }

  @Deprecated
  public String getName() {
    return pipelineInfo.getPipelineId();
  }

  public String getPipelineId() {
    return pipelineInfo.getPipelineId();
  }

  public String getTitle() {
    if (pipelineInfo.getTitle() == null) {
      return pipelineInfo.getPipelineId();
    }
    return pipelineInfo.getTitle();
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

  public Map<String, Object> getMetadata() {
    return pipelineInfo.getMetadata();
  }

  public String getSdcVersion() {
    return pipelineInfo.getSdcVersion();
  }

  public String getSdcId() {
    return pipelineInfo.getSdcId();
  }

  @JsonIgnore
  public com.streamsets.datacollector.store.PipelineInfo getPipelineInfo() {
    return pipelineInfo;
  }
}
