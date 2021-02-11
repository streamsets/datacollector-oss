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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.upgrade.KafkaSecurityUpgradeHelper;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedList;

public class TestKafkaSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    upgrader = SelectorStageUpgrader.createTestInstanceForStageClass(KafkaDSource.class);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testUpgradeV3toV4() throws StageException {
    configs.add(new Config("dataFormat", DataFormat.TEXT));
    configs.add(new Config("metadataBrokerList", "MY_LIST"));
    configs.add(new Config("zookeeperConnect", "MY_ZK_CONNECTION"));
    configs.add(new Config("consumerGroup", "MY_GROUP"));
    configs.add(new Config("topic", "MY_TOPIC"));
    configs.add(new Config("produceSingleRecordPerMessage", false));
    configs.add(new Config("maxBatchSize", 1000));
    configs.add(new Config("maxWaitTime", 10));
    configs.add(new Config("kafkaConsumerConfigs", null));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("removeCtrlChars", false));
    configs.add(new Config("textMaxLineLen", 1024));

    KafkaSourceUpgrader kafkaSourceUpgrader = new KafkaSourceUpgrader();
    kafkaSourceUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    Assert.assertEquals(12, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormat"));
    Assert.assertEquals(DataFormat.TEXT, configValues.get("kafkaConfigBean.dataFormat"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.metadataBrokerList"));
    Assert.assertEquals("MY_LIST", configValues.get("kafkaConfigBean.metadataBrokerList"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.zookeeperConnect"));
    Assert.assertEquals("MY_ZK_CONNECTION", configValues.get("kafkaConfigBean.zookeeperConnect"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.consumerGroup"));
    Assert.assertEquals("MY_GROUP", configValues.get("kafkaConfigBean.consumerGroup"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.topic"));
    Assert.assertEquals("MY_TOPIC", configValues.get("kafkaConfigBean.topic"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.produceSingleRecordPerMessage"));
    Assert.assertEquals(false, configValues.get("kafkaConfigBean.produceSingleRecordPerMessage"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.maxBatchSize"));
    Assert.assertEquals(1000, configValues.get("kafkaConfigBean.maxBatchSize"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.maxWaitTime"));
    Assert.assertEquals(10, configValues.get("kafkaConfigBean.maxWaitTime"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConsumerConfigs"));
    Assert.assertEquals(null, configValues.get("kafkaConfigBean.kafkaConsumerConfigs"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("kafkaConfigBean.dataFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormatConfig.removeCtrlChars"));
    Assert.assertEquals(false, configValues.get("kafkaConfigBean.dataFormatConfig.removeCtrlChars"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormatConfig.textMaxLineLen"));
    Assert.assertEquals(1024, configValues.get("kafkaConfigBean.dataFormatConfig.textMaxLineLen"));
  }

  @Test
  public void testupgradeV6ToV7() throws StageException {
    configs.add(new Config("dataFormat", DataFormat.TEXT));
    configs.add(new Config("metadataBrokerList", "MY_LIST"));
    configs.add(new Config("zookeeperConnect", "MY_ZK_CONNECTION"));
    configs.add(new Config("consumerGroup", "MY_GROUP"));
    configs.add(new Config("topic", "MY_TOPIC"));
    configs.add(new Config("produceSingleRecordPerMessage", false));
    configs.add(new Config("maxBatchSize", 1000));
    configs.add(new Config("maxWaitTime", 10));
    configs.add(new Config("kafkaConsumerConfigs", null));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("removeCtrlChars", false));
    configs.add(new Config("textMaxLineLen", 1024));

    Map<String, String> kafkaOptions = new HashMap<>();
    kafkaOptions.put("auto.offset.reset", "latest");

    configs.add(new Config("kafkaOptions", kafkaOptions));
    Assert.assertTrue(!kafkaOptions.isEmpty());

    KafkaSourceUpgrader kafkaSourceUpgrader = new KafkaSourceUpgrader();
    kafkaSourceUpgrader.upgrade("a", "b", "c", 6, 7, configs);

    Assert.assertEquals(KafkaAutoOffsetReset.LATEST, configs.get(configs.size() - 2).getValue());
    Assert.assertTrue(kafkaOptions.isEmpty());

  }

  @Test
  public void testV7ToV8() {
    Mockito.doReturn(7).when(context).getFromVersion();
    Mockito.doReturn(8).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "kafkaConfigBean.keyCaptureMode", "NONE");
    UpgraderTestUtils.assertExists(configs, "kafkaConfigBean.keyCaptureAttribute", "kafkaMessageKey");
    UpgraderTestUtils.assertExists(configs, "kafkaConfigBean.keyCaptureField", "/kafkaMessageKey");
  }

  @Test
  public void testV9ToV10() {
    Mockito.doReturn(9).when(context).getFromVersion();
    Mockito.doReturn(10).when(context).getToVersion();

    String dataFormatPrefix = "kafkaConfigBean.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }

  @Test
  public void testV10ToV11() {
    Mockito.doReturn(10).when(context).getFromVersion();
    Mockito.doReturn(11).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(
        configs,
        "kafkaConfigBean.provideKeytab",
        false
    );
    UpgraderTestUtils.assertExists(
        configs,
        "kafkaConfigBean.userKeytab",
        ""
    );
    UpgraderTestUtils.assertExists(
        configs,
        "kafkaConfigBean.userPrincipal",
        "user/host@REALM"
    );
  }

  @Test
  public void testV11toV12() {
    KafkaSecurityUpgradeHelper.testUpgradeSecurityOptions(
        upgrader,
        11,
        "kafkaConfigBean",
        "kafkaConsumerConfigs",
        "metadataBrokerList"
    );
  }

  @Test
  public void testV12toV13() {
    Mockito.doReturn(12).when(context).getFromVersion();
    Mockito.doReturn(13).when(context).getToVersion();

    final List<Map<String, String>> kafkaClientConfigs = new LinkedList<>();
    Map<String, String> configMap = new HashMap<>();
    configMap.put("key", "sasl.mechanism");
    configMap.put("value","PLAIN");
    kafkaClientConfigs.add(configMap);

    String stageConfigPath = "kafkaConfigBean";
    String kafkaConfigsPath = stageConfigPath + ".kafkaConsumerConfigs";
    String kafkaSecurityProtocolPath = stageConfigPath+".connectionConfig.connection.securityConfig.securityOption";
    String kafkaMechanismPath = stageConfigPath+".connectionConfig.connection.securityConfig.saslMechanism";

    configs.add(new Config(kafkaConfigsPath, Collections.unmodifiableList(kafkaClientConfigs)));
    configs.add(new Config(kafkaSecurityProtocolPath, "SASL_PLAINTEXT"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, kafkaSecurityProtocolPath, "SASL_PLAINTEXT");
    UpgraderTestUtils.assertExists(configs, kafkaMechanismPath, true);
  }

  @Test
  public void testV13toV14() {
    Mockito.doReturn(13).when(context).getFromVersion();
    Mockito.doReturn(14).when(context).getToVersion();

    String stageConfigPath = "kafkaConfigBean";
    String kafkaSecurityProtocolPath = stageConfigPath+".connectionConfig.connection.securityConfig.securityOption";
    String kafkaMechanismPath = stageConfigPath+".connectionConfig.connection.securityConfig.saslMechanism";

    configs.add(new Config(kafkaSecurityProtocolPath, "SASL_PLAINTEXT"));
    configs.add(new Config(kafkaMechanismPath, true));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, kafkaSecurityProtocolPath, "SASL_PLAINTEXT");
    UpgraderTestUtils.assertExists(configs, kafkaMechanismPath, "PLAIN");
  }
}
