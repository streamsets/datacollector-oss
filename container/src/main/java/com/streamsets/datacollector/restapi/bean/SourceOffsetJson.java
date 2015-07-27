/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.runner.production.SourceOffset;

public class SourceOffsetJson {

  private final SourceOffset sourceOffset;

  public SourceOffsetJson(SourceOffset sourceOffset) {
    this.sourceOffset = sourceOffset;
  }

  @JsonCreator
  public SourceOffsetJson(
    @JsonProperty("offset") String offset) {
    this.sourceOffset = new SourceOffset(offset);
  }

  public String getOffset() {
    return sourceOffset.getOffset();
  }

  @JsonIgnore
  public SourceOffset getSourceOffset() {
    return sourceOffset;
  }
}
