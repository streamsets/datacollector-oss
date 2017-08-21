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
package com.streamsets.pipeline.stage.processor.hbase;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestHBaseProcessorUpgrader {
  @Test
  public void testV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("conf.hBaseConnectionConfig.zookeeperQuorum", "localhost"));
    configs.add(new Config("conf.hBaseConnectionConfig.clientPort", "2181"));
    configs.add(new Config("conf.hBaseConnectionConfig.zookeeperParentZNode", "/hbase"));
    configs.add(new Config("conf.hBaseConnectionConfig.tableName", "test"));
    configs.add(new Config("conf.hBaseConnectionConfig.kerberosAuth", true));
    configs.add(new Config("conf.hBaseConnectionConfig.hbaseUser", null));
    configs.add(new Config("conf.hBaseConnectionConfig.hbaseConfDir", null));
    configs.add(new Config("conf.hBaseConnectionConfig.hbaseConfigs", new ArrayList()));
    configs.add(new Config("conf.mode", "BATCH"));
    Map<String, String> lookupsConfigMap = new LinkedHashMap<>();
    lookupsConfigMap.put("rowExpr", "");
    lookupsConfigMap.put("columnExpr", "");
    lookupsConfigMap.put("timestampExpr", "");
    lookupsConfigMap.put("tables", "");
    lookupsConfigMap.put("outputFieldPath", "");
    List<Map<String, String>> lookupsConfigs = new ArrayList<>();
    lookupsConfigs.add(lookupsConfigMap);
    lookupsConfigs.add(lookupsConfigMap);
    configs.add(new Config("conf.lookups", lookupsConfigs));
    configs.add(new Config("conf.cache.enabled", false));

    StageUpgrader hbaseProcessorUpgrader = new HBaseProcessorUpgrader();

    hbaseProcessorUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(11, configs.size());

    lookupsConfigMap.remove("tables");
    for (Config config : configs) {
      if ("conf.lookups".equals(config.getName())) {
        assertEquals(lookupsConfigs, config.getValue());
      }
    }
  }

  @Test
  public void testV2toV3() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("conf.hBaseConnectionConfig.zookeeperParentZnode", "/hbase"));

    StageUpgrader hbaseTargetUpgrader = new HBaseProcessorUpgrader();
    hbaseTargetUpgrader.upgrade("a", "b", "c", 2, 3, configs);

    assertEquals(1, configs.size());
    assertEquals("conf.hBaseConnectionConfig.zookeeperParentZNode", configs.get(0).getName());

  }
}
