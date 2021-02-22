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

package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.jdbc.connection.upgrader.JdbcConnectionUpgradeTestUtil;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.processor.jdbclookup.JdbcLookupProcessorUpgrader;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestJdbcLookupUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;
  private JdbcConnectionUpgradeTestUtil connectionUpgradeTester;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/JdbcLookupDProcessor.yaml");
    upgrader = new SelectorStageUpgrader("stage", null, yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
    connectionUpgradeTester = new JdbcConnectionUpgradeTestUtil();
  }

  @Test
  public void testUpgradeV1toV2() throws StageException {
    JdbcLookupProcessorUpgrader upgrader = new JdbcLookupProcessorUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 1, 2, configs);

    UpgraderTestUtils.assertAllExist(upgradedConfigs,
        "cacheConfig.enabled",
        "cacheConfig.maxSize",
        "cacheConfig.evictionPolicyType",
        "cacheConfig.expirationTime",
        "cacheConfig.timeUnit"
        );
  }

  @Test
  public void testUpgradeV2toV3() throws StageException {
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    final String queryInterval = "5";
    configs.add(new Config(queryIntervalField, queryInterval));

    JdbcLookupProcessorUpgrader upgrader = new JdbcLookupProcessorUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);

    UpgraderTestUtils.assertExists(upgradedConfigs, "missingValuesBehavior", MissingValuesBehavior.SEND_TO_ERROR);
  }

  @Test
  public void testUpgradeV3toV4() throws StageException {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    connectionUpgradeTester.testJdbcConnectionIntroduction(
        configs,
        upgrader,
        context,
        "hikariConfigBean.",
        "connection."
    );
  }

  @Test
  public void testUpgradeV5toV6() throws StageException {
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    List<Config> upgradedConfigs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(upgradedConfigs, "validateColumnMappings", false);
  }
}
