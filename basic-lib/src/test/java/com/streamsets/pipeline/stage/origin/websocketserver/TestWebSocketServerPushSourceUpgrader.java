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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestWebSocketServerPushSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/WebSocketServerDPushSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", new WebSocketServerPushSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1ToV10() {
    TlsConfigBeanUpgraderTestUtil.testRawKeyStoreConfigsToTlsConfigBeanUpgrade(
        "webSocketConfigs.",
        new WebSocketServerPushSourceUpgrader(),
        10
    );
  }

  @Test
  public void testV10ToV11() {
    Mockito.doReturn(10).when(context).getFromVersion();
    Mockito.doReturn(11).when(context).getToVersion();

    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "responseConfig.dataFormat",
        "responseConfig.dataGeneratorFormatConfig.charset",
        "responseConfig.dataGeneratorFormatConfig.jsonMode"
    );
  }

  @Test
  public void testV11ToV12() {
    Mockito.doReturn(11).when(context).getFromVersion();
    Mockito.doReturn(12).when(context).getToVersion();

    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "responseConfig.sendRawResponse"
    );
  }

  @Test
  public void testV12ToV13() {
    Mockito.doReturn(12).when(context).getFromVersion();
    Mockito.doReturn(13).when(context).getToVersion();

    configs.add(new Config("dataFormatConfig.preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "dataFormatConfig.preserveRootElement", false);
  }

  @Test
  public void testV13ToV14() {
    Mockito.doReturn(13).when(context).getFromVersion();
    Mockito.doReturn(14).when(context).getToVersion();

    String configPrefix = "webSocketConfigs.tlsConfigBean.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, configPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "privateKey", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "certificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, configPrefix + "trustedCertificates", new ArrayList<>());
  }
}
