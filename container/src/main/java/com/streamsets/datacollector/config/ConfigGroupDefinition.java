/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigGroupDefinition {
  private final Set<String> groupNames;
  private final Map<String, List<String>> classNameToGroupsMap;
  private final List<Map<String, String>> groupNameToLabelMapList;

  public ConfigGroupDefinition(Set<String> groupsNames, Map<String, List<String>> classNameToGroupsMap,
    List<Map<String, String>> groupNameToLabelMap) {
    this.groupNames = groupsNames;
    this.classNameToGroupsMap = classNameToGroupsMap;
    this.groupNameToLabelMapList = groupNameToLabelMap;
  }

  public Set<String> getGroupNames() {
    return groupNames;
  }

  public Map<String, List<String>> getClassNameToGroupsMap() {
    return classNameToGroupsMap;
  }

  public List<Map<String, String>> getGroupNameToLabelMapList() {
    return groupNameToLabelMapList;
  }

}
