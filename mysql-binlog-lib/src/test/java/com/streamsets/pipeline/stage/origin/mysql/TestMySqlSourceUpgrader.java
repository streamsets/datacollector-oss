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
import static org.junit.Assert.assertEquals;
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
    assertEquals(2, configs.size());
    HashMap<String, Object> upgraded = new HashMap<>();
    for (Config c : configs) {
      upgraded.put(c.getName(), c.getValue());
    }
    assertEquals(upgraded.get(MysqlDSource.CONFIG_PREFIX + "enableKeepAlive"), true);
    assertEquals(upgraded.get(MysqlDSource.CONFIG_PREFIX + "keepAliveInterval"), 60000);
  }

  @Test
  public void testUpgradeFromV2toV3() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.port", "1234"));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", "secret"));
    configs.add(new Config("config.useSsl", false));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost:1234");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "secret");
  }

  @Test
  public void testUpgradeFromV2toV3WithSSL() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.port", "1234"));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", "secret"));
    configs.add(new Config("config.useSsl", true));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost:1234?useSsl=true");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "secret");
  }

  @Test
  public void testUpgradeFromV2toV3WithoutPort() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", "secret"));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "secret");
  }

  @Test
  public void testUpgradeFromV2toV3WithEmptyPort() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.port", ""));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", "secret"));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "secret");
  }

  @Test
  public void testUpgradeFromV2toV3WithNullPort() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.port", null));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", "secret"));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "secret");
  }

  @Test
  public void testUpgradeFromV2toV3WithoutUsername() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(2, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", false);
  }

  @Test
  public void testUpgradeFromV2toV3WithNullUsername() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.username", null));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(3, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.username", null);
    assertExists(configs, "connection.useCredentials", false);
  }

  @Test
  public void testUpgradeFromV2toV3WithEmptyUsername() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.username", ""));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(3, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.username", "");
    assertExists(configs, "connection.useCredentials", false);
  }

  @Test
  public void testUpgradeFromV2toV3WithoutPassword() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.username", "user"));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(3, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
  }

  @Test
  public void testUpgradeFromV2toV3WithNullPassword() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", null));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", null);
  }

  @Test
  public void testUpgradeFromV2toV3WithEmptyPassword() {
    doReturn(2).when(context).getFromVersion();
    doReturn(3).when(context).getToVersion();

    configs.add(new Config("config.hostname", "localhost"));
    configs.add(new Config("config.username", "user"));
    configs.add(new Config("config.password", ""));
    configs = mySQLBinLogSourceUpgrader.upgrade(configs, context);

    assertEquals(4, configs.size());
    assertExists(configs, "connection.connectionString", "jdbc:mysql://localhost");
    assertExists(configs, "connection.useCredentials", true);
    assertExists(configs, "connection.username", "user");
    assertExists(configs, "connection.password", "");
  }
}
