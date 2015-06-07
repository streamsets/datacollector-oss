/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.Map;

public class ConfigGroupDefinitionJson {

  private final com.streamsets.pipeline.config.ConfigGroupDefinition configGroupDefinition;

  @JsonCreator
  public ConfigGroupDefinitionJson(
    @JsonProperty("classNameToGroupsMap") Map<String, List<String>> classNameToGroupsMap,
    @JsonProperty("groupNameToLabelMapList") List<Map<String, String>> groupNameToLabelMap) {
    this.configGroupDefinition = new com.streamsets.pipeline.config.ConfigGroupDefinition(null, classNameToGroupsMap,
      groupNameToLabelMap);
  }

  public ConfigGroupDefinitionJson(com.streamsets.pipeline.config.ConfigGroupDefinition configGroupDefinition) {
    Utils.checkNotNull(configGroupDefinition, "configGroupDefinition");
    this.configGroupDefinition = configGroupDefinition;
  }

  public Map<String, List<String>> getClassNameToGroupsMap() {
    return configGroupDefinition.getClassNameToGroupsMap();
  }

  public List<Map<String, String>> getGroupNameToLabelMapList() {
    return configGroupDefinition.getGroupNameToLabelMapList();
  }

  @JsonIgnore
  public com.streamsets.pipeline.config.ConfigGroupDefinition getConfigGroupDefinition() {
    return configGroupDefinition;
  }
}