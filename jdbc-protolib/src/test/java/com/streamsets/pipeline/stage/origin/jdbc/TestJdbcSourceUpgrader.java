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

package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.jdbc.connection.upgrader.JdbcConnectionUpgradeTestUtil;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestJdbcSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;
  private JdbcConnectionUpgradeTestUtil connectionUpgradeTester;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/JdbcDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", null, yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
    connectionUpgradeTester = new JdbcConnectionUpgradeTestUtil();
  }

  @Test
  public void testUpgradeV9toV10() throws StageException {
    List<Config> configs = new ArrayList<>();
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    final String queryInterval = "5";
    configs.add(new Config(queryIntervalField, queryInterval));

    JdbcSourceUpgrader upgrader = new JdbcSourceUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 9, 10, configs);

    UpgraderTestUtils.assertNoneExist(
        upgradedConfigs,
        "commonSourceConfigBean.queriesPerSecond",
        "commonSourceConfigBean.queryInterval"
    );
    UpgraderTestUtils.assertExists(configs, "queryInterval", queryInterval);
  }

  @Test
  public void testUpgradeV12toV13() throws StageException {
    Mockito.doReturn(12).when(context).getFromVersion();
    Mockito.doReturn(13).when(context).getToVersion();

    connectionUpgradeTester.testJdbcConnectionIntroduction(
        configs,
        upgrader,
        context,
        "hikariConfigBean.",
        "connection."
    );
  }
}
