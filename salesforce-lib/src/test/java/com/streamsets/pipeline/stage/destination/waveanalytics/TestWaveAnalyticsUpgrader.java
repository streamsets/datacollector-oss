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
package com.streamsets.pipeline.stage.destination.waveanalytics;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.salesforce.SubscriptionType;
import com.streamsets.pipeline.stage.processor.lookup.ForceLookupProcessorUpgrader;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.lib.waveanalytics.WaveAnalyticsConfigBean.APPEND_TIMESTAMP;
import static com.streamsets.pipeline.stage.destination.waveanalytics.WaveAnalyticsDTarget
    .WAVE_ANALYTICS_DESTINATION_CONFIG_BEAN_PREFIX;

public class TestWaveAnalyticsUpgrader {
  private static final String WAVE_ANALYTICS_APPEND_TIMESTAMP = WAVE_ANALYTICS_DESTINATION_CONFIG_BEAN_PREFIX + "." + APPEND_TIMESTAMP;

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/WaveAnalyticsDTarget.yaml");
    upgrader = new SelectorStageUpgrader("stage", new WaveAnalyticsUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testUpgradeV1toV2NoAppendTimestamp() throws StageException {
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    WaveAnalyticsUpgrader waveAnalyticsUpgrader = new WaveAnalyticsUpgrader();
    waveAnalyticsUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(WAVE_ANALYTICS_APPEND_TIMESTAMP, config.getName());
    Assert.assertEquals(true, config.getValue());
  }

  @Ignore
  public void testUpgradeV1toV2AppendTimestampTrue() throws StageException {
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    configs.add(new Config(WAVE_ANALYTICS_APPEND_TIMESTAMP, true));

    WaveAnalyticsUpgrader waveAnalyticsUpgrader = new WaveAnalyticsUpgrader();
    waveAnalyticsUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(WAVE_ANALYTICS_APPEND_TIMESTAMP, config.getName());
    Assert.assertEquals(true, config.getValue());
  }

  @Ignore
  public void testUpgradeV1toV2AppendTimestampFalse() throws StageException {
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    configs.add(new Config(WAVE_ANALYTICS_APPEND_TIMESTAMP, false));

    WaveAnalyticsUpgrader waveAnalyticsUpgrader = new WaveAnalyticsUpgrader();
    waveAnalyticsUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(WAVE_ANALYTICS_APPEND_TIMESTAMP, config.getName());
    Assert.assertEquals(false, config.getValue());
  }

  @Test
  public void testV2ToV3Upgrade() {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    String configPrefix = "conf.mutualAuth.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, configPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "privateKey", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "certificateChain", new ArrayList<>());
  }

  @Test
  public void testV3toV4Upgrade() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    configs.add(new Config("conf.username", "v1"));
    configs.add(new Config("conf.password", "v2"));
    configs.add(new Config("conf.authEndpoint", "v3"));
    configs.add(new Config("conf.apiVersion", "v4"));
    configs.add(new Config("conf.useProxy", "v5"));
    configs.add(new Config("conf.proxyHostname", "v6"));
    configs.add(new Config("conf.proxyPort", "v7"));
    configs.add(new Config("conf.useProxyCredentials", "v8"));
    configs.add(new Config("conf.proxyRealm", "v9"));
    configs.add(new Config("conf.proxyUsername", "v10"));
    configs.add(new Config("conf.proxyPassword", "v11"));
    configs.add(new Config("conf.mutualAuth.useMutualAuth", "v12"));
    configs.add(new Config("conf.mutualAuth.useRemoteKeyStore", "v13"));
    configs.add(new Config("conf.mutualAuth.keyStoreFilePath", "v14"));
    configs.add(new Config("conf.mutualAuth.privateKey", "v15"));
    configs.add(new Config("conf.mutualAuth.certificateChain", "v16"));
    configs.add(new Config("conf.mutualAuth.keyStoreType", "v17"));
    configs.add(new Config("conf.mutualAuth.keyStorePassword", "v18"));
    configs.add(new Config("conf.mutualAuth.keyStoreAlgorithm", "v19"));
    configs.add(new Config("conf.mutualAuth.underlyingConfig", "v20"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.connection.username", "v1");
    UpgraderTestUtils.assertExists(configs, "conf.connection.password", "v2");
    UpgraderTestUtils.assertExists(configs, "conf.connection.authEndpoint", "v3");
    UpgraderTestUtils.assertExists(configs, "conf.connection.apiVersion", "v4");
    UpgraderTestUtils.assertExists(configs, "conf.connection.useProxy", "v5");
    UpgraderTestUtils.assertExists(configs, "conf.connection.proxyHostname", "v6");
    UpgraderTestUtils.assertExists(configs, "conf.connection.proxyPort", "v7");
    UpgraderTestUtils.assertExists(configs, "conf.connection.useProxyCredentials", "v8");
    UpgraderTestUtils.assertExists(configs, "conf.connection.proxyRealm", "v9");
    UpgraderTestUtils.assertExists(configs, "conf.connection.proxyUsername", "v10");
    UpgraderTestUtils.assertExists(configs, "conf.connection.proxyPassword", "v11");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.useMutualAuth", "v12");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.useRemoteKeyStore", "v13");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.keyStoreFilePath", "v14");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.privateKey", "v15");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.certificateChain", "v16");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.keyStoreType", "v17");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.keyStorePassword", "v18");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.keyStoreAlgorithm", "v19");
    UpgraderTestUtils.assertExists(configs, "conf.connection.mutualAuth.underlyingConfig", "v20");

    Assert.assertEquals(20, configs.size());
  }
}
