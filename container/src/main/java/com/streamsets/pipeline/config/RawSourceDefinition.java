/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class RawSourceDefinition {

  private final String rawSourcePreviewerClass;
  private final String mimeType;
  List<ConfigDefinition> configDefinitions;

  @JsonCreator
  public RawSourceDefinition(
      @JsonProperty("rawSourcePreviewerClass") String rawSourcePreviewerClass,
      @JsonProperty("mimeType") String mimeType,
      @JsonProperty("configDefinitions") List<ConfigDefinition> configDefinitions) {
    this.rawSourcePreviewerClass = rawSourcePreviewerClass;
    this.configDefinitions = configDefinitions;
    this.mimeType = mimeType;
  }

  public String getRawSourcePreviewerClass() {
    return rawSourcePreviewerClass;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public String getMimeType() {
    return mimeType;
  }
}
