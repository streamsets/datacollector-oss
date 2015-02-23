/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import java.util.List;
import java.util.Map;

public class ConfigGroupDefinition {

  private final Map<String, List<String>> classNameToGroupsMap;
  private final List<Map<String, String>> groupNameToLabelMapList;

  public ConfigGroupDefinition( Map<String, List<String>> classNameToGroupsMap,
    List<Map<String, String>> groupNameToLabelMap) {
    this.classNameToGroupsMap = classNameToGroupsMap;
    this.groupNameToLabelMapList = groupNameToLabelMap;
  }

  public Map<String, List<String>> getClassNameToGroupsMap() {
    return classNameToGroupsMap;
  }

  public List<Map<String, String>> getGroupNameToLabelMapList() {
    return groupNameToLabelMapList;
  }

}
