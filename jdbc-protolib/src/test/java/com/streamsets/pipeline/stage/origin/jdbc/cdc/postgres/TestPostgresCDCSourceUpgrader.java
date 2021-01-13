/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.streamsets.pipeline.lib.jdbc.connection.upgrader.JdbcConnectionUpgradeTestUtil;

public class TestPostgresCDCSourceUpgrader {

  private StageUpgrader postgresCDCSourceUpgrader;
  JdbcConnectionUpgradeTestUtil connectionUpgradeTester;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/PostgresCDCSource.yaml");
    postgresCDCSourceUpgrader = new SelectorStageUpgrader("stage", new PostgresCDCSourceUpgrader(), yamlResource);
    connectionUpgradeTester = new JdbcConnectionUpgradeTestUtil();
  }

  @Test
  public void testV1ToV2() {
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    List<Config> configs = postgresCDCSourceUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals(1, configs.size());

    Config configValue = configs.get(0);
    Assert.assertEquals("postgresCDCConfigBean.maxBatchWaitTime", configValue.getName());
    Assert.assertEquals(15000, configValue.getValue());
  }

  @Test
  public void testV2ToV3() {
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("postgresCDCConfigBean.baseConfigBean.maxBatchSize", 500));

    configs = postgresCDCSourceUpgrader.upgrade(configs, context);

    Assert.assertEquals(3, configs.size());

    for (Config config : configs) {
      switch (config.getName()) {
        case "postgresCDCConfigBean.baseConfigBean.maxBatchSize":
          Assert.assertEquals(500, config.getValue());
          break;
        case "postgresCDCConfigBean.generatorQueueMaxSize":
          Assert.assertEquals(2500, config.getValue());
          break;
        case "postgresCDCConfigBean.statusInterval":
          Assert.assertEquals(30, config.getValue());
          break;
        default:
          Assert.fail("Unexpected config :" + config.getName());
          break;
      }
    }
  }

  @Test
  public void testV3ToV4() {
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    List<Config> configs = new ArrayList<>();

    connectionUpgradeTester.testJdbcConnectionIntroduction(
        configs,
        postgresCDCSourceUpgrader,
        context,
        "hikariConf.",
        "connection."
    );
  }

}
