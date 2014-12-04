/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class PipelineRevInfo {
  private final Date date;
  private final String user;
  private final String rev;
  private final String tag;
  private final String description;
  private final boolean valid;

  public PipelineRevInfo(PipelineInfo info) {
    date = info.getLastModified();
    user = info.getLastModifier();
    rev = info.getLastRev();
    tag = null;
    description = null;
    valid = info.isValid();
  }

  @JsonCreator
  public PipelineRevInfo(
      @JsonProperty("date") Date date,
      @JsonProperty("user") String user,
      @JsonProperty("rev") String rev,
      @JsonProperty("tag") String tag,
      @JsonProperty("description") String description,
      @JsonProperty("valid") boolean valid) {
    this.date = date;
    this.user = user;
    this.rev = rev;
    this.tag = tag;
    this.description = description;
    this.valid = valid;
  }

  public Date getDate() {
    return date;
  }

  public String getUser() {
    return user;
  }

  public String getRev() {
    return rev;
  }

  public String getTag() {
    return tag;
  }

  public String getDescription() {
    return description;
  }

  public boolean isValid() {
    return valid;
  }

}
