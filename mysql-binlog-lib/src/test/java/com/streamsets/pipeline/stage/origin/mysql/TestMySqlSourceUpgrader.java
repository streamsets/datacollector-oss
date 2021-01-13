/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.streamsets.pipeline.config.upgrade.UpgraderTestUtils.assertExists;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestMySqlSourceUpgrader {
  private StageUpgrader mySQLBinLogSourceUpgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/MysqlDSource.yaml");
    mySQLBinLogSourceUpgrader = new SelectorStageUpgrader("stage", new MySqlSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = mock(StageUpgrader.Context.class);
  }

  @Test
  public void testUpgradeFromV1toV2() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader upgrader = new MySqlSourceUpgrader();

    try {
       upgrader.upgrade("", "MySQL Bin Log Origin", "test", 1, 2, configs);
    } catch (StageException e) {
      Assert.fail("Exception should not be thrown:" + e.getMessage());
    }
    Assert.assertEquals(2, configs.size());
    HashMap<String, Object> upgraded = new HashMap<>();
    for (Config c : configs) {
      upgraded.put(c.getName(), c.getValue());
    }
    Assert.assertEquals(upgraded.get(MysqlDSource.CONFIG_PREFIX + "enableKeepAlive"), true);
    Assert.assertEquals(upgraded.get(MysqlDSource.CONFIG_PREFIX + "keepAliveInterval"), 60000);
  }

  @Test
  public void testUpgradeFromV2toV3() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.port", "1234"));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", "secret"));
    configs.add(new Config("config.useSsl", true));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertExists(configs, "connection.hostname", "localhost");
    assertExists(configs, "connection.port", "1234");
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "secret");
    assertExists(configs, "connection.useSsl", true);
  }
}
