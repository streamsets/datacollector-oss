/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.bean;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.restapi.bean.ConfigGroupDefinitionJson;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestConfigGroupDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testConfigGroupDefinitionBeanNull() {
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
    group2.add("C");
    group2.add("D");
    classNameToGroupsMap.put("g2", group2);

    List<Map<String, String>> groupNameToLabelMapList = new ArrayList<>();
    Map<String, String> map1 = new HashMap<>();
    map1.put("g1", "Group One");
    Map<String, String> map2 = new HashMap<>();
    map2.put("g2", "Group Two");
    groupNameToLabelMapList.add(map1);
    groupNameToLabelMapList.add(map2);

    com.streamsets.datacollector.config.ConfigGroupDefinition configGroupDefinition =
      new ConfigGroupDefinition(ImmutableSet.of("A", "B", "C", "C"), classNameToGroupsMap, groupNameToLabelMapList);

    ConfigGroupDefinitionJson configGroupDefinitionJsonBean =
      new ConfigGroupDefinitionJson(configGroupDefinition);

    Assert.assertEquals(configGroupDefinition.getClassNameToGroupsMap(),
      configGroupDefinitionJsonBean.getClassNameToGroupsMap());
    Assert.assertEquals(configGroupDefinition.getGroupNameToLabelMapList(),
      configGroupDefinitionJsonBean.getGroupNameToLabelMapList());
  }

}
