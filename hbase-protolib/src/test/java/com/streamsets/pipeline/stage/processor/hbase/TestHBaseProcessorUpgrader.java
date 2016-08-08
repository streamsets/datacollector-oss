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

package com.streamsets.pipeline.stage.processor.hbase;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestHBaseProcessorUpgrader {
  @Test
  public void testV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("conf.hBaseConnectionConfig.zookeeperQuorum", "localhost"));
    configs.add(new Config("conf.hBaseConnectionConfig.clientPort", "2181"));
    configs.add(new Config("conf.hBaseConnectionConfig.zookeeperParentZnode", "/hbase"));
    configs.add(new Config("conf.hBaseConnectionConfig.tableName", "test"));
    configs.add(new Config("conf.hBaseConnectionConfig.kerberosAuth", true));
    configs.add(new Config("conf.hBaseConnectionConfig.hbaseUser", null));
    configs.add(new Config("conf.hBaseConnectionConfig.hbaseConfDir", null));
    configs.add(new Config("conf.hBaseConnectionConfig.hbaseConfigs", new ArrayList()));
    configs.add(new Config("conf.mode", "BATCH"));
    List lookupsConfigs = new ArrayList<>();
    Map<String, String> lookupsConfigMap = new LinkedHashMap<>();
    lookupsConfigMap.put("rowExpr", "");
    lookupsConfigMap.put("columnExprr", "");
    lookupsConfigMap.put("timestampExpr", "");
    lookupsConfigMap.put("tables", "");
    lookupsConfigMap.put("outputFieldPath", "");
    lookupsConfigs.add(lookupsConfigMap);
    configs.add(new Config("conf.lookups", lookupsConfigs));
    configs.add(new Config("conf.cache.enabled", false));

    HBaseProcessorUpgrader hbaseProcessorUpgrader = new HBaseProcessorUpgrader();

    hbaseProcessorUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(11, configs.size());

    lookupsConfigMap.remove("tables");
    Assert.assertEquals(configs.get(10).getValue(), lookupsConfigs);
  }
}
