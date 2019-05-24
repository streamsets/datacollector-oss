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
package com.streamsets.pipeline.stage.destination.hbase;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHBaseTargetUpgrader {
  @Test
  public void testV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("zookeeperQuorum", "localhost"));
    configs.add(new Config("clientPort", "80"));
    configs.add(new Config("zookeeperParentZnode", "/hbase"));
    configs.add(new Config("tableName", "test"));
    configs.add(new Config("kerberosAuth", true));
    configs.add(new Config("hbaseUser", ""));
    configs.add(new Config("hbaseConfDir", ""));
    configs.add(new Config("hbaseConfigs", new ArrayList()));

    StageUpgrader hbaseTargetUpgrader = new HBaseTargetUpgrader();
    hbaseTargetUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    assertEquals(8, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    final String ZOOKEEPER_QUORUM = "hBaseConnectionConfig.zookeeperQuorum";
    final String CLIENT_PORT = "hBaseConnectionConfig.clientPort";
    final String ZOOKEEPER_PARENT_ZNODE = "hBaseConnectionConfig.zookeeperParentZnode";
    final String TABLE_NAME = "hBaseConnectionConfig.tableName";
    final String KERBEROS_AUTH = "hBaseConnectionConfig.kerberosAuth";
    final String HBASE_USER = "hBaseConnectionConfig.hbaseUser";
    final String HBASE_CONF_DIR = "hBaseConnectionConfig.hbaseConfDir";
    final String HBASE_CONFIGS = "hBaseConnectionConfig.hbaseConfigs";

    Assert.assertTrue(configValues.containsKey(ZOOKEEPER_QUORUM));
    assertEquals("localhost", configValues.get(ZOOKEEPER_QUORUM));

    Assert.assertTrue(configValues.containsKey(CLIENT_PORT));
    assertEquals("80", configValues.get(CLIENT_PORT));

    Assert.assertTrue(configValues.containsKey(ZOOKEEPER_PARENT_ZNODE));
    assertEquals("/hbase", configValues.get(ZOOKEEPER_PARENT_ZNODE));

    Assert.assertTrue(configValues.containsKey(TABLE_NAME));
    assertEquals("test", configValues.get(TABLE_NAME));

    Assert.assertTrue(configValues.containsKey(KERBEROS_AUTH));
    assertEquals(true, configValues.get(KERBEROS_AUTH));

    Assert.assertTrue(configValues.containsKey(HBASE_USER));
    assertEquals("", configValues.get(HBASE_USER));

    Assert.assertTrue(configValues.containsKey(HBASE_CONF_DIR));
    assertEquals("", configValues.get(HBASE_CONF_DIR));

    Assert.assertTrue(configValues.containsKey(HBASE_CONFIGS));
    assertEquals(new ArrayList(), configValues.get(HBASE_CONFIGS));
  }

  @Test
  public void testV2toV3() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("hBaseConnectionConfig.zookeeperParentZnode", "/hbase"));

    StageUpgrader hbaseTargetUpgrader = new HBaseTargetUpgrader();
    hbaseTargetUpgrader.upgrade("a", "b", "c", 2, 3, configs);

    assertEquals(1, configs.size());
    assertEquals("hBaseConnectionConfig.zookeeperParentZNode", configs.get(0).getName());

  }

  @Test
  public void testV3toV4() throws Exception {
    List<Config> configs = new ArrayList<>();

    StageUpgrader hbaseTargetUpgrader = new HBaseTargetUpgrader();
    hbaseTargetUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    assertEquals(1, configs.size());
    assertEquals("validateTableExistence", configs.get(0).getName());

    Assert.assertTrue(configValues.containsKey("validateTableExistence"));
    assertEquals(true, configValues.get("validateTableExistence"));
  }
}
