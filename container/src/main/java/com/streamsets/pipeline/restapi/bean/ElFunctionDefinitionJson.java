/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElFunctionDefinition;

import java.util.List;

public class ElFunctionDefinitionJson {

  private final ElFunctionDefinition elFunctionDefinition;

  public ElFunctionDefinitionJson(
    @JsonProperty("name") String name,
    @JsonProperty("description") String description,
    @JsonProperty("group") String group,
    @JsonProperty("returnType") String returnType,
    @JsonProperty("elFunctionArgumentDefinition") List<ElFunctionArgumentDefinitionJson> elFunctionArgumentDefinition) {
    this.elFunctionDefinition = new ElFunctionDefinition(group, name, description,
                                                         BeanHelper.unwrapElFunctionArgumentDefinitions(elFunctionArgumentDefinition),
                                                         returnType
    );
  }

  public ElFunctionDefinitionJson(ElFunctionDefinition elFunctionDefinition) {
    Utils.checkNotNull(elFunctionDefinition, "elFunctionDefinition");
    this.elFunctionDefinition = elFunctionDefinition;
  }

  public String getId() {
    return elFunctionDefinition.getId();
  }

  public String getName() {
    return elFunctionDefinition.getName();
  }

  public String getDescription() {
    return elFunctionDefinition.getDescription();
  }

  public String getGroup() {
    return elFunctionDefinition.getGroup();
  }

  public String getReturnType() {
    return elFunctionDefinition.getReturnType();
  }

  public List<ElFunctionArgumentDefinitionJson> getElFunctionArgumentDefinition() {
    return BeanHelper.wrapElFunctionArgumentDefinitions(elFunctionDefinition.getElFunctionArgumentDefinition());
  }

  @JsonIgnore
  public ElFunctionDefinition getElFunctionDefinition() {
    return elFunctionDefinition;
  }
}
