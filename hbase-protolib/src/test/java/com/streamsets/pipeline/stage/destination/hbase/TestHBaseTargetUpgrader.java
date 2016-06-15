/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

    HBaseTargetUpgrader hbaseTargetUpgrader = new HBaseTargetUpgrader();
    hbaseTargetUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(8, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    final String ZOOKEEPERQUORUM = "hBaseConnectionConfig.zookeeperQuorum";
    final String CLIENTPORT = "hBaseConnectionConfig.clientPort";
    final String ZOOKEEPERPARENTZNODE = "hBaseConnectionConfig.zookeeperParentZnode";
    final String TABLENAME = "hBaseConnectionConfig.tableName";
    final String KERBEROSAUTH = "hBaseConnectionConfig.kerberosAuth";
    final String HBASEUSER = "hBaseConnectionConfig.hbaseUser";
    final String HBASECONFDIR = "hBaseConnectionConfig.hbaseConfDir";
    final String HBASECONFIGS = "hBaseConnectionConfig.hbaseConfigs";

    Assert.assertTrue(configValues.containsKey(ZOOKEEPERQUORUM));
    Assert.assertEquals("localhost", configValues.get(ZOOKEEPERQUORUM));

    Assert.assertTrue(configValues.containsKey(CLIENTPORT));
    Assert.assertEquals("80", configValues.get(CLIENTPORT));

    Assert.assertTrue(configValues.containsKey(ZOOKEEPERPARENTZNODE));
    Assert.assertEquals("/hbase", configValues.get(ZOOKEEPERPARENTZNODE));

    Assert.assertTrue(configValues.containsKey(TABLENAME));
    Assert.assertEquals("test", configValues.get(TABLENAME));

    Assert.assertTrue(configValues.containsKey(KERBEROSAUTH));
    Assert.assertEquals(true, configValues.get(KERBEROSAUTH));

    Assert.assertTrue(configValues.containsKey(HBASEUSER));
    Assert.assertEquals("", configValues.get(HBASEUSER));

    Assert.assertTrue(configValues.containsKey(HBASECONFDIR));
    Assert.assertEquals("", configValues.get(HBASECONFDIR));

    Assert.assertTrue(configValues.containsKey(HBASECONFIGS));
    Assert.assertEquals(new ArrayList(), configValues.get(HBASECONFIGS));
  }
}
