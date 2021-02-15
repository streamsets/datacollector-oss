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
package com.streamsets.datacollector.configupgrade;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@RunWith(Parameterized.class)
public class TestConnectionConfigurationUpgrader extends BaseTestConnectionConfigurationUpgrader {

  @Test
  public void testVersionSame() {
    prep("type1", 1, "not-exist");
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, Collections.emptyList());
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testVersionConnectionNew() {
    prep("type1", 1, "not-exist");
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 2, Collections.emptyList());
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("CONTAINER_0902", issues.get(0).getErrorCode());
  }

  @Test
  public void testUpgrade() {
    prep("type1", 2, "upgrader/TestConnectionConfigurationUpgrader1.yaml");
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("prop1.subprop1", "original-value-1"));
    configs.add(new Config("prop2.subprop1", "original-value-2"));
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, configs);
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(0, issues.size());
    configs = connConfig.getConfiguration();
    Assert.assertEquals(3, configs.size());
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
    Assert.assertEquals(2, connConfig.getVersion());
  }

  @Test
  public void testUpgradeFileNotExist() {
    prep("type1", 2, "not-exist");
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, Collections.emptyList());
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("YAML_UPGRADER_07", issues.get(0).getErrorCode());
  }

  @Test
  public void testUpgradeFileInvalid() {
    prep("type1", 2, "upgrader/TestConnectionConfigurationUpgrader2.yaml");
    ConnectionConfiguration connConfig = new ConnectionConfiguration("type1", 1, Collections.emptyList());
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("CONTAINER_0900", issues.get(0).getErrorCode());
  }
}
