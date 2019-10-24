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
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestRabbitSourceUpgrader {
  @Test
  public void testUpgradeV1ToV2() throws Exception{
    final Joiner JOINER = Joiner.on(".");

    List<Config> configs = new ArrayList<>();
    configs.add(new Config(JOINER.join("conf", "uri"), "amqp://localhost:5672"));
    configs.add(new Config(JOINER.join("conf", "consumerTag"), ""));
    configs.add(new Config(JOINER.join("conf", "dataFormat"), DataFormat.JSON));

    RabbitSourceUpgrader upgrader = new RabbitSourceUpgrader();
    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(4, configs.size());
    boolean isValid = false;
    for (Config config : configs) {
      if (JOINER.join("conf", "produceSingleRecordPerMessage").equals(config.getName())) {
        isValid = !((boolean)config.getValue());
      }
    }
    Assert.assertTrue("Should contain produceSingleRecordPerMessage and its value set to false", isValid);

  }

  @Test
  public void testUpgradeV3ToV4() throws Exception {
    List<Config> configs = new ArrayList<>();
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/RabbitDSource.yaml");
    StageUpgrader upgrader = new SelectorStageUpgrader("stage", new RabbitSourceUpgrader(), yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);
    Assert.assertEquals("Upgrader must generate 13 config parameters.", 13, configs.size());

    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.tlsEnabled", false);
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.keyStoreFilePath", "");
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.keyStoreType", KeyStoreType.JKS.toString());
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.keyStorePassword", "");
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.keyStoreAlgorithm", "SunX509");
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.trustStoreFilePath", "");
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.trustStoreType", KeyStoreType.JKS.toString());
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.trustStorePassword", "");
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.trustStoreAlgorithm", "SunX509");
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.useDefaultProtocols", true);
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.protocols", new LinkedList<String>());
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.useDefaultCiperSuites", true);
    UpgraderTestUtils.assertExists(configs, "conf.tlsConfig.cipherSuites", new LinkedList<String>());
  }
}
