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

public class ConfigGroupDefinition {

  private final Map<String, List<String>> classNameToGroupsMap;
  private final Map<String, String> groupNameToLabelMap;

  @JsonCreator
  public ConfigGroupDefinition(
    @JsonProperty("classNameToGroupsMap") Map<String, List<String>> classNameToGroupsMap,
    @JsonProperty("groupNameToLabelMap") Map<String, String> groupNameToLabelMap) {
    this.classNameToGroupsMap = classNameToGroupsMap;
    this.groupNameToLabelMap = groupNameToLabelMap;
  }

  public Map<String, List<String>> getClassNameToGroupsMap() {
    return classNameToGroupsMap;
  }

  public Map<String, String> getGroupNameToLabelMap() {
    return groupNameToLabelMap;
  }

 /* @Override
  public String toString() {
    return Utils.format("ConfigGroupDefinition[configGroupClass='{}']", getConfigGroupClass());
  }*/
}
