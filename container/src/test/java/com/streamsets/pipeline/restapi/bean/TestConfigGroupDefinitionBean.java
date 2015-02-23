/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.config.ConfigGroupDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestConfigGroupDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testConfigGroupDefinitionBeanNull() {
    ConfigGroupDefinitionJson configGroupDefinitionJsonBean =
      new ConfigGroupDefinitionJson(null);
  }

  @Test
  public void testConfigGroupDefinitionBean() {
    Map<String, List<String>> classNameToGroupsMap = new HashMap<>();
    List<String> group1 = new ArrayList<>();
    group1.add("A");
    group1.add("B");
    classNameToGroupsMap.put("g1", group1);
    List<String> group2 = new ArrayList<>();
    group1.add("C");
    group1.add("D");
    classNameToGroupsMap.put("g2", group2);

    List<Map<String, String>> groupNameToLabelMapList = new ArrayList<>();
    Map<String, String> map1 = new HashMap<>();
    map1.put("g1", "Group One");
    Map<String, String> map2 = new HashMap<>();
    map2.put("g2", "Group Two");
    groupNameToLabelMapList.add(map1);
    groupNameToLabelMapList.add(map2);

    com.streamsets.pipeline.config.ConfigGroupDefinition configGroupDefinition =
      new ConfigGroupDefinition(classNameToGroupsMap, groupNameToLabelMapList);

    ConfigGroupDefinitionJson configGroupDefinitionJsonBean =
      new ConfigGroupDefinitionJson(configGroupDefinition);

    Assert.assertEquals(configGroupDefinition.getClassNameToGroupsMap(),
      configGroupDefinitionJsonBean.getClassNameToGroupsMap());
    Assert.assertEquals(configGroupDefinition.getGroupNameToLabelMapList(),
      configGroupDefinitionJsonBean.getGroupNameToLabelMapList());
  }

  @Test
  public void testConfigGroupDefinitionBeanConstructorWithArgs() {
    Map<String, List<String>> classNameToGroupsMap = new HashMap<>();
    List<String> group1 = new ArrayList<>();
    group1.add("A");
    group1.add("B");
    classNameToGroupsMap.put("g1", group1);
    List<String> group2 = new ArrayList<>();
    group1.add("C");
    group1.add("D");
    classNameToGroupsMap.put("g2", group2);

    List<Map<String, String>> groupNameToLabelMapList = new ArrayList<>();
    Map<String, String> map1 = new HashMap<>();
    map1.put("g1", "Group One");
    Map<String, String> map2 = new HashMap<>();
    map2.put("g2", "Group Two");
    groupNameToLabelMapList.add(map1);
    groupNameToLabelMapList.add(map2);

    com.streamsets.pipeline.config.ConfigGroupDefinition configGroupDefinition =
      new ConfigGroupDefinition(classNameToGroupsMap, groupNameToLabelMapList);

    ConfigGroupDefinitionJson configGroupDefinitionJsonBean =
      new ConfigGroupDefinitionJson(classNameToGroupsMap, groupNameToLabelMapList);

    Assert.assertEquals(configGroupDefinition.getClassNameToGroupsMap(),
      configGroupDefinitionJsonBean.getClassNameToGroupsMap());
    Assert.assertEquals(configGroupDefinition.getGroupNameToLabelMapList(),
      configGroupDefinitionJsonBean.getGroupNameToLabelMapList());

    Assert.assertEquals(configGroupDefinition.getClassNameToGroupsMap(),
      configGroupDefinitionJsonBean.getConfigGroupDefinition().getClassNameToGroupsMap());
    Assert.assertEquals(configGroupDefinition.getGroupNameToLabelMapList(),
      configGroupDefinitionJsonBean.getConfigGroupDefinition().getGroupNameToLabelMapList());
  }
}
