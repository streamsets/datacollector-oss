/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.cassandra;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestCassandraTargetUpgrader {

  public static final String prefix = "conf.";

  private StageUpgrader upgrader;

  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/CassandraDTarget.yaml");
    upgrader = new SelectorStageUpgrader("stage", new CassandraTargetUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  /**
   * Property disableBatchInsert is renamed to enableBatches and its value is negated
   * Property requestTimeout is renamed to writeTimeout
   */
  @Test
  public void testUpgradeV5ToV6() {
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    String oldBatchesName = "disableBatchInsert";
    String oldWriteTimeoutName = "requestTimeout";
    String newBatchesName = "enableBatches";
    String newWriteTimeoutName = "writeTimeout";
    int testTimeoutValue = 145;

    // initial disableBatchInsert false value
    configs.add(new Config(prefix + oldBatchesName, false));
    configs.add(new Config(prefix + oldWriteTimeoutName, testTimeoutValue));
    configs = upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertNoneExist(configs, prefix + oldBatchesName);
    UpgraderTestUtils.assertExists(configs, prefix + newBatchesName, true);

    // write timeout renaming
    UpgraderTestUtils.assertNoneExist(configs, prefix + oldWriteTimeoutName);
    UpgraderTestUtils.assertExists(configs, prefix + newWriteTimeoutName, testTimeoutValue);

    // initial disableBatchInsert false value
    configs = new ArrayList<>();
    configs.add(new Config(prefix + oldBatchesName, true));
    configs = upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertNoneExist(configs, prefix + oldBatchesName);
    UpgraderTestUtils.assertExists(configs, prefix + newBatchesName, false);
  }

  @Test
  public void testV6ToV7Upgrade() {
    Mockito.doReturn(6).when(context).getFromVersion();
    Mockito.doReturn(7).when(context).getToVersion();

    String configPrefix = "conf.tlsConfig.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, configPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "privateKey", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "certificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, configPrefix + "trustedCertificates", new ArrayList<>());
  }
}
