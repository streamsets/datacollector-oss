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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestHiveQueryExecutorUpgrader {
  @Test
  @SuppressWarnings("unchecked")
  public void testUpgradeV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config(HiveQueryExecutorUpgrader.HIVE_QUERY_EXECUTOR_CONFIG_PREFIX + HiveQueryExecutorUpgrader.QUERY, "select 1"));

    HiveQueryExecutorUpgrader upgrader = new HiveQueryExecutorUpgrader();
    upgrader.upgrade("", "",  "hive executor", 1, 2, configs);

    Assert.assertEquals(2, configs.size());
    for (Config config : configs) {
      Object value = config.getValue();
      if (config.getName().equals(HiveQueryExecutorUpgrader.HIVE_QUERY_EXECUTOR_CONFIG_PREFIX + HiveQueryExecutorUpgrader.QUERIES)) {
        Assert.assertTrue(value instanceof List);
        List<String> queries = ((List<String>)value);
        Assert.assertEquals(1, queries.size());
        Assert.assertEquals("select 1", queries.get(0));
      } else if (config.getName().equals(HiveQueryExecutorUpgrader.HIVE_QUERY_EXECUTOR_CONFIG_PREFIX + HiveQueryExecutorUpgrader.STOP_ON_QUERY_FAILURE)) {
        Assert.assertTrue((boolean)value);
      } else {
        Assert.fail(Utils.format("Unexpected upgrade config {}", config.getName()));
      }
    }

  }
}
