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

package com.streamsets.pipeline.stage.origin.multikafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.KafkaSecurityUpgradeHelper;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.LinkedList;

public class TestMultiKafkaSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    upgrader = SelectorStageUpgrader.createTestInstanceForStageClass(MultiKafkaDSource.class);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV2ToV3() {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.keyCaptureMode", "NONE");
    UpgraderTestUtils.assertExists(configs, "conf.keyCaptureAttribute", "kafkaMessageKey");
    UpgraderTestUtils.assertExists(configs, "conf.keyCaptureField", "/kafkaMessageKey");
  }

  @Test
  public void testV3ToV4() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.dataFormatConfig.basicAuth", "");
  }

  @Test
  public void testV4ToV5() {
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    String dataFormatPrefix = "conf.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }

  @Test
  public void testV5ToV6() {
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.timestampsEnabled", false);
    UpgraderTestUtils.assertExists(
        configs,
        "conf.provideKeytab",
        false
    );
    UpgraderTestUtils.assertExists(
        configs,
        "conf.userKeytab",
        ""
    );
    UpgraderTestUtils.assertExists(
        configs,
        "conf.userPrincipal",
        "user/host@REALM"
    );
  }

  @Test
  public void testV6toV7() {
    KafkaSecurityUpgradeHelper.testUpgradeSecurityOptions(
        upgrader,
        6,
        "conf",
        "kafkaOptions",
        "brokerURI"
    );
  }

  @Test
  public void testV7toV8() {
    Mockito.doReturn(7).when(context).getFromVersion();
    Mockito.doReturn(8).when(context).getToVersion();

    final List<Map<String, String>> kafkaClientConfigs = new LinkedList<>();
    Map<String, String> configMap = new HashMap<>();
    configMap.put("key", "sasl.mechanism");
    configMap.put("value","PLAIN");
    kafkaClientConfigs.add(configMap);

    String stageConfigPath = "conf";
    String kafkaConfigsPath = stageConfigPath + ".kafkaOptions";
    String kafkaSecurityProtocolPath = stageConfigPath+".connectionConfig.connection.securityConfig.securityOption";
    String kafkaMechanismPath = stageConfigPath+".connectionConfig.connection.securityConfig.saslMechanism";

    configs.add(new Config(kafkaConfigsPath, Collections.unmodifiableList(kafkaClientConfigs)));
    configs.add(new Config(kafkaSecurityProtocolPath, "SASL_PLAINTEXT"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, kafkaSecurityProtocolPath, "SASL_PLAINTEXT");
    UpgraderTestUtils.assertExists(configs, kafkaMechanismPath, true);
  }

  @Test
  public void testV8toV9() {
    Mockito.doReturn(8).when(context).getFromVersion();
    Mockito.doReturn(9).when(context).getToVersion();

    String stageConfigPath = "conf";
    String kafkaSecurityProtocolPath = stageConfigPath+".connectionConfig.connection.securityConfig.securityOption";
    String kafkaMechanismPath = stageConfigPath+".connectionConfig.connection.securityConfig.saslMechanism";

    configs.add(new Config(kafkaSecurityProtocolPath, "SASL_PLAINTEXT"));
    configs.add(new Config(kafkaMechanismPath, true));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, kafkaSecurityProtocolPath, "SASL_PLAINTEXT");
    UpgraderTestUtils.assertExists(configs, kafkaMechanismPath, "PLAIN");
  }
}
