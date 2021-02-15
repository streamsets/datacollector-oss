/*
 * Copyright 2021 StreamSets Inc.
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

package com.streamsets.pipeline.lib.kafka.connection;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.configupgrade.BaseTestConnectionConfigurationUpgrader;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class TestKafkaConnectionUpgrader extends BaseTestConnectionConfigurationUpgrader {

  @Test
  public void testV1ToV2_saslMechanismNotExists() {
    prep(KafkaConnection.TYPE, 2, "upgrader/KafkaConnectionUpgrader.yaml");
    ConnectionConfiguration connConfig = new ConnectionConfiguration(KafkaConnection.TYPE, 1, Collections.emptyList());
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(0, issues.size());
    List<Config> configs = connConfig.getConfiguration();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertEquals(configs.get(0).getName(), "securityConfig.saslMechanism");
    Assert.assertEquals(configs.get(0).getValue(), "GSSAPI");
  }

  @Test
  public void testV1ToV2_saslMechanismExists() {
    prep(KafkaConnection.TYPE, 2, "upgrader/KafkaConnectionUpgrader.yaml");
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("securityConfig.saslMechanism", true));
    ConnectionConfiguration connConfig = new ConnectionConfiguration(KafkaConnection.TYPE, 1, configs);
    List<Issue> issues = run(connConfig);
    Assert.assertEquals(0, issues.size());
    configs = connConfig.getConfiguration();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertEquals(configs.get(0).getName(), "securityConfig.saslMechanism");
    Assert.assertEquals(configs.get(0).getValue(), "PLAIN");
  }
}
