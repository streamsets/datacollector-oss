/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.UUID;

public class PipelineInfo {
  private String name;
  private String description;
  private Date created;
  private Date lastModified;
  private String creator;
  private String lastModifier;
  private String lastRev;
  private UUID uuid;
  private boolean valid;

  @JsonCreator
  public PipelineInfo(
      @JsonProperty("name") String name,
      @JsonProperty("description") String description,
      @JsonProperty("created") Date created,
      @JsonProperty("lastModified") Date lastModified,
      @JsonProperty("creator") String creator,
      @JsonProperty("lastModifier") String lastModifier,
      @JsonProperty("lastRev") String lastRev,
      @JsonProperty("uuid") UUID uuid,
      @JsonProperty("valid") boolean valid) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.lastModified = lastModified;
    this.creator = creator;
    this.lastModifier = lastModifier;
    this.lastRev = lastRev;
    this.uuid = uuid;
    this.valid = valid;
  }

  public PipelineInfo(PipelineInfo pipelineInfo, String description, Date lastModified, String lastModifier,
      String lastRev, UUID uuid, boolean valid) {
    this(pipelineInfo.getName(), description, pipelineInfo.getCreated(), lastModified,
         pipelineInfo.getCreator(), lastModifier, lastRev, uuid, valid);
  }

  public String getName() {
    return name;
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
}
