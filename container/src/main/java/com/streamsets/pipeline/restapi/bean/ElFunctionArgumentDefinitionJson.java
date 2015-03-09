/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElFunctionArgumentDefinition;

public class ElFunctionArgumentDefinitionJson {

  private final ElFunctionArgumentDefinition elFunctionArgumentDefinition;

  public ElFunctionArgumentDefinitionJson(
    @JsonProperty("name") String name,
    @JsonProperty("type") String type) {
    this.elFunctionArgumentDefinition = new ElFunctionArgumentDefinition(name, type);
  }

  public ElFunctionArgumentDefinitionJson(ElFunctionArgumentDefinition elFunctionArgumentDefinition) {
    Utils.checkNotNull(elFunctionArgumentDefinition, "elFunctionArgumentDefinition");
    this.elFunctionArgumentDefinition = elFunctionArgumentDefinition;
  }

  public String getName() {
    return elFunctionArgumentDefinition.getName();
  }

  public String getType() {
    return elFunctionArgumentDefinition.getType();
  }

  @JsonIgnore
  public ElFunctionArgumentDefinition getElFunctionArgumentDefinition() {
    return elFunctionArgumentDefinition;
  }
}
