/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner.production;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceOffset {

  private final String offset;

  @JsonCreator
  public SourceOffset(
      @JsonProperty("offset") String offset) {
    this.offset = offset;
  }

  public String getOffset() {
    return offset;
  }
}
