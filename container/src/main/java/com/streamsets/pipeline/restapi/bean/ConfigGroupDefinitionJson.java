/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.Map;

public class ConfigGroupDefinitionJson {

  private final com.streamsets.pipeline.config.ConfigGroupDefinition configGroupDefinition;

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

}