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
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestRedisSourceUpgrader {

  private StageUpgrader redisSourceUpgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/RedisDSource.yaml");
    redisSourceUpgrader = new SelectorStageUpgrader("stage", new RedisSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1toV2() throws StageException {
    configs.add(new Config("redisOriginConfigBean.readStrategy", "BATCH"));
    configs.add(new Config("redisOriginConfigBean.queueName", ""));
    configs.add(new Config("redisOriginConfigBean.advancedConfig.keysPattern", "*"));
    configs.add(new Config("redisOriginConfigBean.advancedConfig.namespaceSeparator", ":"));
    configs.add(new Config("redisOriginConfigBean.advancedConfig.executionTimeout", "60"));
    configs.add(new Config("redisOriginConfigBean.advancedConfig.connectionTimeout", "60"));

    RedisSourceUpgrader redisSourceUpgrader = new RedisSourceUpgrader();
    redisSourceUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(1, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("redisOriginConfigBean.connectionTimeout"));
    Assert.assertEquals("60", configValues.get("redisOriginConfigBean.connectionTimeout"));
  }

  @Test
  public void testV3ToV4() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    String dataFormatPrefix = "redisOriginConfigBean.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = redisSourceUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }
}
