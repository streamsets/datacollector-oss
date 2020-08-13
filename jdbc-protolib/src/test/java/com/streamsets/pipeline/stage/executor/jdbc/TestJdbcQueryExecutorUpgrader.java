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

package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.jdbc.connection.upgrader.JdbcConnectionUpgradeTestUtil;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestJdbcQueryExecutorUpgrader {
  public static final String prefix = "config.";

  private StageUpgrader upgrader;

  private List<Config> configs;
  private StageUpgrader.Context context;
  private JdbcConnectionUpgradeTestUtil connectionUpgradeTester;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/JdbcQueryDExecutor.yaml");
    upgrader = new SelectorStageUpgrader("stage", new JdbcQueryExecutorUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
    connectionUpgradeTester = new JdbcConnectionUpgradeTestUtil();
  }

  /**
   * Property batchCommit is added as false
   */
  @Test
  public void testUpgradeV1ToV2() {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    String batchCommit = "batchCommit";

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, prefix + batchCommit, false);
  }

  /**
   * Property parallel is added as false
   */
  @Test
  public void testUpgradeV2ToV3() {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    String parallel = "parallel";

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, prefix + parallel, false);
  }

  /**
   * Property parallel is renamed to isParallel
   * Property query is converted to a List
   */
  @Test
  public void testUpgradeV3ToV4() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    String parallelOldName = "parallel";
    String parallelNewName = "isParallel";
    String queryConfigOldName = "query";
    String queryConfigNewName = "queries";
    String queryConfigValue = "Select * from a";

    configs.add(new Config(prefix + parallelOldName, true));
    configs.add(new Config(prefix + queryConfigOldName, queryConfigValue));
    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(2, configs.size());

    UpgraderTestUtils.assertNoneExist(configs, prefix + parallelOldName);
    UpgraderTestUtils.assertExists(configs, prefix + parallelNewName, true);
    UpgraderTestUtils.assertExists(configs, prefix + queryConfigNewName);

    for (Config config : configs) {
      if (config.getName().equals(prefix + queryConfigNewName)) {
        Assert.assertEquals(queryConfigValue, ((List<String>) config.getValue()).get(0));
      }
    }
  }

  @Test
  public void testUpgradeV4toV5() throws StageException {
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    connectionUpgradeTester.testJdbcConnectionIntroduction(
        configs,
        upgrader,
        context,
        "config.hikariConfigBean.",
        "connection."
    );
  }
}