/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RawSourceDefinitionJson {

  private final com.streamsets.pipeline.config.RawSourceDefinition rawSourceDefinition;

  @JsonCreator
  public RawSourceDefinitionJson(
    @JsonProperty("rawSourcePreviewerClass") String rawSourcePreviewerClass,
    @JsonProperty("mimeType") String mimeType,
    @JsonProperty("configDefinitions") List<ConfigDefinitionJson> configDefinitionJsons) {
    this.rawSourceDefinition = new com.streamsets.pipeline.config.RawSourceDefinition(rawSourcePreviewerClass, mimeType,
      BeanHelper.unwrapConfigDefinitions(configDefinitionJsons));
  }

  public RawSourceDefinitionJson(com.streamsets.pipeline.config.RawSourceDefinition rawSourceDefinition) {
    this.rawSourceDefinition = rawSourceDefinition;
  }

  public String getRawSourcePreviewerClass() {
    return rawSourceDefinition.getRawSourcePreviewerClass();
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(rawSourceDefinition.getConfigDefinitions());
  }

  public String getMimeType() {
    return rawSourceDefinition.getMimeType();
  }

  @JsonIgnore
  public com.streamsets.pipeline.config.RawSourceDefinition getRawSourceDefinition() {
    return rawSourceDefinition;
  }
}