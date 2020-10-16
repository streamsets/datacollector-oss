/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.connection.TestConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;

public class TestConnectionDefinitionExtractor {

  private static final StageLibraryDefinition MOCK_LIB_DEF =
      new StageLibraryDefinition(
          TestStageDefinitionExtractor.class.getClassLoader(),
          "Test Library Name", "Test Library Description",
          new Properties(), null, null, null
      );

  private void validateGroups(ConfigGroupDefinition groupsDef) {
    List<String> groupNames = new ArrayList<>();
    for (TestConnectionDef.TestConnectionGroups group : TestConnectionDef.TestConnectionGroups.values()) {
      groupNames.add(group.toString());
    }
    // Assert groupNames
    Assert.assertEquals(TestConnectionDef.TestConnectionGroups.values().length, groupsDef.getGroupNames().size());
    for (String groupName : groupsDef.getGroupNames()) {
      Assert.assertTrue(groupNames.contains(groupName));
    }
    // Assert classNameToGroupsMap
    Assert.assertEquals(1, groupsDef.getClassNameToGroupsMap().keySet().size());
    String groupsClassName = TestConnectionDef.TestConnectionGroups.class.getName();
    Assert.assertNotNull(groupsDef.getClassNameToGroupsMap().get(groupsClassName));
    List<String> nameToGroupsMap = groupsDef.getClassNameToGroupsMap().get(groupsClassName);
    Assert.assertEquals(TestConnectionDef.TestConnectionGroups.values().length, nameToGroupsMap.size());
    for (String groupName : nameToGroupsMap) {
      Assert.assertTrue(groupNames.contains(groupName));
    }
    // Assert groupNameToLabelMapList
    List<Map<String, String>> nameToLabelMapList = groupsDef.getGroupNameToLabelMapList();
    Assert.assertEquals(TestConnectionDef.TestConnectionGroups.values().length, nameToLabelMapList.size());
    for (Map<String, String> nameToLabel : nameToLabelMapList) {
      Assert.assertEquals(2, nameToLabel.keySet().size());
      Assert.assertNotNull(nameToLabel.get("name"));
      Assert.assertNotNull(nameToLabel.get("label"));
      Assert.assertTrue(groupNames.contains(nameToLabel.get("name")));
      Assert.assertEquals(
              TestConnectionDef.TestConnectionGroups.valueOf(nameToLabel.get("name")).getLabel(),
              nameToLabel.get("label")
      );
    }
  }

  @Test
  public void testExtractConnection() {
    ConnectionDefinition def = ConnectionDefinitionExtractor.get()
        .extract(MOCK_LIB_DEF, TestConnectionDef.TestConnection.class);

    // Assert connection fields
    Assert.assertEquals(1, def.getVersion());
    Assert.assertEquals("Test Connection", def.getLabel());
    Assert.assertEquals("Connects to Test Connection", def.getDescription());
    Assert.assertEquals("TEST_CON_TYPE", def.getType());
    Assert.assertEquals("upgrader/TestConnection.yaml", def.getUpgrader());

    // Assert connection configurations
    Assert.assertNotNull(def.getConfigDefinitions());
    // there is a third config ("bufferSize"), but it should have been hidden
    Assert.assertEquals(2, def.getConfigDefinitions().size());
    Assert.assertEquals("host", def.getConfigDefinitions().get(0).getName());
    Assert.assertEquals("Test Connection Host", def.getConfigDefinitions().get(0).getLabel());
    Assert.assertEquals("Test Connection Host Description", def.getConfigDefinitions().get(0).getDescription());
    Assert.assertEquals("com.streamsets.test.host", def.getConfigDefinitions().get(0).getDefaultValue());
    Assert.assertEquals("G1", def.getConfigDefinitions().get(0).getGroup());
    Assert.assertEquals(true, def.getConfigDefinitions().get(0).isRequired());
    Assert.assertEquals("port", def.getConfigDefinitions().get(1).getName());
    Assert.assertEquals("Test Connection Port", def.getConfigDefinitions().get(1).getLabel());
    Assert.assertEquals("Test Connection Port Description", def.getConfigDefinitions().get(1).getDescription());
    Assert.assertEquals(8080, def.getConfigDefinitions().get(1).getDefaultValue());
    Assert.assertEquals("G1", def.getConfigDefinitions().get(1).getGroup());
    Assert.assertEquals(false, def.getConfigDefinitions().get(1).isRequired());
    Assert.assertEquals(2, def.getConfigDefinitionsMap().size());
    Assert.assertEquals(def.getConfigDefinitions().get(0), def.getConfigDefinitionsMap().get("host"));
    Assert.assertEquals(def.getConfigDefinitions().get(1), def.getConfigDefinitionsMap().get("port"));
    Assert.assertArrayEquals(new ConnectionEngine[]{ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER},
        def.getSupportedEngines());
    Assert.assertEquals(TestStageDefinitionExtractor.class.getClassLoader(), def.getClassLoader());

    // Assert groups
    ConfigGroupDefinition groupsDef = def.getConfigGroupDefinition();
    validateGroups(groupsDef);
  }
}
