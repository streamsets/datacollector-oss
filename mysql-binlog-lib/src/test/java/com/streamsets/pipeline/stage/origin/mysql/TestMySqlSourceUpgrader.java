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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestMySqlSourceUpgrader {

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
    Assert.assertEquals(upgraded.get(MysqlSourceConfig.CONFIG_PREFIX + "enableKeepAlive"), true);
    Assert.assertEquals(upgraded.get(MysqlSourceConfig.CONFIG_PREFIX + "keepAliveInterval"), 60000);
  }
}
