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
package com.streamsets.datacollector.validation;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.DetachedConnectionConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestDetachedConnectionValidator {

  private StageLibraryTask createStageLibraryTask(ConnectionDefinition connectionDefinition) {
    StageLibraryTask libTask = Mockito.mock(StageLibraryTask.class);
    Mockito.when(libTask.getConnection(connectionDefinition.getType())).thenReturn(connectionDefinition);
    Mockito.when(libTask.getConnections()).thenReturn(Collections.singletonList(connectionDefinition));
    return libTask;
  }

  private ConnectionDefinition createConnectionDefinition(
      String type,
      int version,
      String upgraderDef,
      List<ConfigDefinition> configDefinitions
  ) {
    ConnectionDefinition connectionDefinition = Mockito.mock(ConnectionDefinition.class);
    Mockito.when(connectionDefinition.getType()).thenReturn(type);
    Mockito.when(connectionDefinition.getVersion()).thenReturn(version);
    Mockito.when(connectionDefinition.getUpgrader()).thenReturn(upgraderDef);
    Mockito.when(connectionDefinition.getConfigDefinitions()).thenReturn(configDefinitions);
    Map<String, ConfigDefinition> configDefinitionsMap = configDefinitions.stream()
        .collect(Collectors.toMap(ConfigDefinition::getName, Function.identity()));
    Mockito.when(connectionDefinition.getConfigDefinitionsMap()).thenReturn(configDefinitionsMap);
    Mockito.when(connectionDefinition.getClassLoader()).thenReturn(getClass().getClassLoader());
    return connectionDefinition;
  }

  private ConfigDefinition createConfigDefinition(String name, Object defaultValue) {
    ConfigDefinition configDefinition = Mockito.mock(ConfigDefinition.class);
    Mockito.when(configDefinition.getName()).thenReturn(name);
    Mockito.when(configDefinition.getDefaultValue()).thenReturn(defaultValue);
    return configDefinition;
  }

  @Test
  public void testValidateUpgradeFileNotExist() {
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, Collections.emptyList());
    ConnectionDefinition connectionDefinition = createConnectionDefinition("type1", 2, "not-exist", Collections.emptyList());
    StageLibraryTask libTask = createStageLibraryTask(connectionDefinition);
    DetachedConnectionConfiguration detachedConnConfig = new DetachedConnectionConfiguration(connConfig);
    DetachedConnectionValidator validator = new DetachedConnectionValidator(libTask, detachedConnConfig);
    validator.validate(false);
    List<Issue> issues = validator.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("YAML_UPGRADER_07", issues.get(0).getErrorCode());
  }

  @Test
  public void testValidate() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("prop1.subprop1", "original-value-1"));
    configs.add(new Config("prop2.subprop1", "original-value-2"));
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, configs);
    List<ConfigDefinition> configDefinitions = new ArrayList<>();
    configDefinitions.add(createConfigDefinition("prop1.subprop1", "fromDefault1"));
    configDefinitions.add(createConfigDefinition("prop1.subprop2", "fromDefault2"));
    configDefinitions.add(createConfigDefinition("prop2.subprop1", "fromDefault3"));
    configDefinitions.add(createConfigDefinition("prop3", "fromDefault4"));
    ConnectionDefinition connectionDefinition = createConnectionDefinition(
        "type1",
        2,
        "upgrader/TestDetachedConnectionValidatorUpgrader.yaml",
        configDefinitions
    );
    StageLibraryTask libTask = createStageLibraryTask(connectionDefinition);
    DetachedConnectionConfiguration detachedConnConfig = new DetachedConnectionConfiguration(connConfig);
    DetachedConnectionValidator validator = new DetachedConnectionValidator(libTask, detachedConnConfig);
    validator.validate(false);
    List<Issue> issues = validator.getIssues().getIssues();
    Assert.assertEquals(0, issues.size());
    configs = connConfig.getConfiguration();
    Assert.assertEquals(2, configs.size());
    configs.sort(Comparator.comparing(Config::getName));
    // Upgrade sets prop1.subprop1, replacing original value
    Assert.assertEquals("prop1.subprop1", configs.get(0).getName());
    Assert.assertEquals("original-value-1", configs.get(0).getValue());
    // Upgrade adds new prop1.subprop2
    Assert.assertEquals("prop2.subprop1", configs.get(1).getName());
    Assert.assertEquals("original-value-2", configs.get(1).getValue());
    // Current and latest available versions
    Assert.assertEquals(1, detachedConnConfig.getConnectionConfiguration().getVersion());
    Assert.assertEquals(2, detachedConnConfig.getLatestAvailableVersion());
  }

  @Test
  public void testValidateRequiredValues() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("prop1", "original-value-1"));
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, configs);
    List<ConfigDefinition> configDefinitions = new ArrayList<>();
    configDefinitions.add(createConfigDefinition("prop1", "fromDefault1"));
    Mockito.when(configDefinitions.get(0).isRequired()).thenReturn(true);
    configDefinitions.add(createConfigDefinition("prop2", "fromDefault2"));
    Mockito.when(configDefinitions.get(1).isRequired()).thenReturn(true);
    configDefinitions.add(createConfigDefinition("prop3", null));
    Mockito.when(configDefinitions.get(2).isRequired()).thenReturn(true);
    ConnectionDefinition connectionDefinition = createConnectionDefinition(
        "type1",
        1,
        "not-exist",
        configDefinitions
    );
    StageLibraryTask libTask = createStageLibraryTask(connectionDefinition);
    DetachedConnectionConfiguration detachedConnConfig = new DetachedConnectionConfiguration(connConfig);
    DetachedConnectionValidator validator = new DetachedConnectionValidator(libTask, detachedConnConfig);
    validator.validate(false);
    // All 3 props are required: prop1 is set by the user, prop2 is set by the defaults, and prop3 is unset
    List<Issue> issues = validator.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("VALIDATION_0007", issues.get(0).getErrorCode());
    Assert.assertEquals("prop3", issues.get(0).getConfigName());
  }

  @Test
  public void testValidateNumber() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("prop1", 1));
    configs.add(new Config("prop3", "hello"));
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, configs);
    List<ConfigDefinition> configDefinitions = new ArrayList<>();
    configDefinitions.add(createConfigDefinition("prop1", 1));
    Mockito.when(configDefinitions.get(0).getType()).thenReturn(ConfigDef.Type.NUMBER);
    Mockito.when(configDefinitions.get(0).getMax()).thenReturn(100L);
    configDefinitions.add(createConfigDefinition("prop2", 2));
    Mockito.when(configDefinitions.get(1).getType()).thenReturn(ConfigDef.Type.NUMBER);
    Mockito.when(configDefinitions.get(1).getMax()).thenReturn(100L);
    configDefinitions.add(createConfigDefinition("prop3", "hello"));
    Mockito.when(configDefinitions.get(2).getType()).thenReturn(ConfigDef.Type.NUMBER);
    Mockito.when(configDefinitions.get(2).getMax()).thenReturn(100L);
    ConnectionDefinition connectionDefinition = createConnectionDefinition(
        "type1",
        1,
        "not-exist",
        configDefinitions
    );
    StageLibraryTask libTask = createStageLibraryTask(connectionDefinition);
    DetachedConnectionConfiguration detachedConnConfig = new DetachedConnectionConfiguration(connConfig);
    DetachedConnectionValidator validator = new DetachedConnectionValidator(libTask, detachedConnConfig);
    validator.validate(false);
    // All 3 props are numbers: prop1 and prop2 are numbers, but prop3 is a string
    List<Issue> issues = validator.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("VALIDATION_0009", issues.get(0).getErrorCode());
    Assert.assertEquals("prop3", issues.get(0).getConfigName());
  }

  @Test
  public void testValidateForceUpgrade() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("prop1.subprop1", "original-value-1"));
    configs.add(new Config("prop2.subprop1", "original-value-2"));
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, configs);
    List<ConfigDefinition> configDefinitions = new ArrayList<>();
    configDefinitions.add(createConfigDefinition("prop1.subprop1", "fromDefault1"));
    configDefinitions.add(createConfigDefinition("prop1.subprop2", "fromDefault2"));
    configDefinitions.add(createConfigDefinition("prop2.subprop1", "fromDefault3"));
    configDefinitions.add(createConfigDefinition("prop3", "fromDefault4"));
    ConnectionDefinition connectionDefinition = createConnectionDefinition(
        "type1",
        2,
        "upgrader/TestDetachedConnectionValidatorUpgrader.yaml",
        configDefinitions
    );
    StageLibraryTask libTask = createStageLibraryTask(connectionDefinition);
    DetachedConnectionConfiguration detachedConnConfig = new DetachedConnectionConfiguration(connConfig);
    DetachedConnectionValidator validator = new DetachedConnectionValidator(libTask, detachedConnConfig);
    validator.validate(true);
    List<Issue> issues = validator.getIssues().getIssues();
    Assert.assertEquals(0, issues.size());
    configs = connConfig.getConfiguration();
    Assert.assertEquals(4, configs.size());
    configs.sort(Comparator.comparing(Config::getName));
    // Upgrade sets prop1.subprop1, replacing original value
    Assert.assertEquals("prop1.subprop1", configs.get(0).getName());
    Assert.assertEquals("fromUpgrader1", configs.get(0).getValue());
    // Upgrade adds new prop1.subprop2
    Assert.assertEquals("prop1.subprop2", configs.get(1).getName());
    Assert.assertEquals("fromUpgrader2", configs.get(1).getValue());
    // Upgrade leaves other properties alone
    Assert.assertEquals("prop2.subprop1", configs.get(2).getName());
    Assert.assertEquals("original-value-2", configs.get(2).getValue());
    // Default only sets values for properties not provided by user or upgrade
    Assert.assertEquals("prop3", configs.get(3).getName());
    Assert.assertEquals("fromDefault4", configs.get(3).getValue());
    // Current and latest available versions
    Assert.assertEquals(2, detachedConnConfig.getConnectionConfiguration().getVersion());
    Assert.assertEquals(2, detachedConnConfig.getLatestAvailableVersion());
  }
}
