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
package com.streamsets.datacollector.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineInfo implements Serializable {
  private String pipelineId;
  private String title;
  private String description;
  private Date created;
  private Date lastModified;
  private String creator;
  private String lastModifier;
  private String lastRev;
  private UUID uuid;
  private boolean valid;
  private Map<String, Object> metadata;
  private String sdcVersion;
  private String sdcId;

  @JsonCreator
  public PipelineInfo(
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
      @JsonProperty("sdcVersion") String sdcVersion,
      @JsonProperty("sdcId") String sdcId
  ) {
    this.pipelineId = pipelineId;
    this.title = title;
    this.description = description;
    this.created = created;
    this.lastModified = lastModified;
    this.creator = creator;
    this.lastModifier = lastModifier;
    this.lastRev = lastRev;
    this.uuid = uuid;
    this.valid = valid;
    this.metadata = metadata;
    this.sdcVersion = sdcVersion;
    this.sdcId = sdcId;
  }

  public PipelineInfo(
      PipelineInfo pipelineInfo,
      String title,
      String description,
      Date lastModified,
      String lastModifier,
      String lastRev,
      UUID uuid, boolean valid,
      Map<String, Object> metadata,
      String sdcVersion,
      String sdcId
  ) {
    this(pipelineInfo.getPipelineId(), title, description, pipelineInfo.getCreated(), lastModified,
         pipelineInfo.getCreator(), lastModifier, lastRev, uuid, valid, metadata, sdcVersion, sdcId);
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public void setPipelineId(String pipelineId) {
    this.pipelineId = pipelineId;
  }

  public String getTitle() {
    return title;
  }

  public String getDescription() {
    return description;
  }

  public Date getCreated() {
    return created;
  }

  public Date getLastModified() {
    return lastModified;
  }

  public String getCreator() {
    return creator;
  }

  public String getLastModifier() {
    return lastModifier;
  }

  public String getLastRev() {
    return lastRev;
  }

  public UUID getUuid() {
    return uuid;
  }

  public boolean isValid() {
    return valid;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public String getSdcVersion() {
    return sdcVersion;
  }

  public String getSdcId() {
    return sdcId;
  }

  public void setSdcVersion(String sdcVersion) {
    this.sdcVersion = sdcVersion;
  }
}
