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
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.upgrade.KafkaSecurityUpgradeHelper;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestKafkaTargetUpgrader {

  @Test
  public void testUpgradeV1toV2() throws StageException {

    Map<String, String> kafkaProducerConfig = generateV1ProducerConfigs();
    List<Config> configs = generateV1Configs(kafkaProducerConfig);

    KafkaTargetUpgrader kafkaTargetUpgrader = new KafkaTargetUpgrader();
    kafkaTargetUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(24, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for(Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.metadataBrokerList"));
    Assert.assertEquals("localhost:9001,localhost:9002,localhost:9003", configValues.get("kafkaConfigBean.kafkaConfig.metadataBrokerList"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.runtimeTopicResolution"));
    Assert.assertEquals(true, configValues.get("kafkaConfigBean.kafkaConfig.runtimeTopicResolution"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.topicExpression"));
    Assert.assertEquals("${record:value('/topic')}", configValues.get("kafkaConfigBean.kafkaConfig.topicExpression"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.topicWhiteList"));
    Assert.assertEquals("*", configValues.get("kafkaConfigBean.kafkaConfig.topicWhiteList"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.topic"));
    Assert.assertEquals(null, configValues.get("kafkaConfigBean.kafkaConfig.topic"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.partitionStrategy"));
    Assert.assertEquals(PartitionStrategy.EXPRESSION, configValues.get("kafkaConfigBean.kafkaConfig.partitionStrategy"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.partition"));
    Assert.assertEquals("${record:value('/partition') % 5}", configValues.get("kafkaConfigBean.kafkaConfig.partition"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.singleMessagePerBatch"));
    Assert.assertEquals(true, configValues.get("kafkaConfigBean.kafkaConfig.singleMessagePerBatch"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConfig.kafkaProducerConfigs"));
    Assert.assertEquals(kafkaProducerConfig, configValues.get("kafkaConfigBean.kafkaConfig.kafkaProducerConfigs"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormat"));
    Assert.assertEquals(DataFormat.TEXT, configValues.get("kafkaConfigBean.dataFormat"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.csvFileFormat"));
    Assert.assertEquals(CsvMode.EXCEL, configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.csvFileFormat"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.csvHeader"));
    Assert.assertEquals(CsvHeader.WITH_HEADER, configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.csvHeader"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));
    Assert.assertEquals(false, configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.jsonMode"));
    Assert.assertEquals(JsonMode.ARRAY_OBJECTS, configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.jsonMode"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.textFieldPath"));
    Assert.assertEquals("/myField", configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.textFieldPath"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));
    Assert.assertEquals(true, configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.avroSchema"));
    Assert.assertEquals("hello!!", configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.avroSchema"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.includeSchema"));
    Assert.assertEquals(true, configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.includeSchema"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));
    Assert.assertEquals("/binaryField", configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));

    //newly added configs

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));
    Assert.assertEquals('|', configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));
    Assert.assertEquals('\\', configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));
    Assert.assertEquals('\"', configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));

    Assert.assertEquals("NULL", configValues.get("kafkaConfigBean.dataGeneratorFormatConfig.avroCompression"));

  }

  @Test
  public void testUpgradeV2toV3() throws Exception {
    Map<String, String> kafkaProducerConfig = generateV1ProducerConfigs();
    List<Config> configs = generateV1Configs(kafkaProducerConfig);

    KafkaTargetUpgrader kafkaTargetUpgrader = new KafkaTargetUpgrader();
    kafkaTargetUpgrader.upgrade("a", "b", "c", 1, 3, configs);

    for (Config config : configs) {
      assertFalse(config.getName().contains("kafkaConfig"));
    }
  }

  private Map<String, String> generateV1ProducerConfigs() {
    Map<String, String> kafkaProducerConfig = new HashMap<>();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    return kafkaProducerConfig;
  }

  private List<Config> generateV1Configs(Map<String, String> kafkaProducerConfig) {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("metadataBrokerList", "localhost:9001,localhost:9002,localhost:9003"));
    configs.add(new Config("runtimeTopicResolution", true));
    configs.add(new Config("topicExpression", "${record:value('/topic')}"));
    configs.add(new Config("topicWhiteList", "*"));
    configs.add(new Config("topic", null));
    configs.add(new Config("partitionStrategy", PartitionStrategy.EXPRESSION));
    configs.add(new Config("partition", "${record:value('/partition') % 5}"));
    configs.add(new Config("singleMessagePerBatch", true));
    configs.add(new Config("kafkaProducerConfigs", kafkaProducerConfig));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("dataFormat", DataFormat.TEXT));
    configs.add(new Config("csvFileFormat", CsvMode.EXCEL));
    configs.add(new Config("csvHeader", CsvHeader.WITH_HEADER));
    configs.add(new Config("csvReplaceNewLines", false));
    configs.add(new Config("jsonMode", JsonMode.ARRAY_OBJECTS));
    configs.add(new Config("textFieldPath", "/myField"));
    configs.add(new Config("textEmptyLineIfNull", true));
    configs.add(new Config("avroSchema", "hello!!"));
    configs.add(new Config("includeSchema", true));
    configs.add(new Config("binaryFieldPath", "/binaryField"));
    return configs;
  }


  @Test
  public void testUpgradeV3toV4() throws Exception {
    Map<String, String> kafkaProducerConfig = generateV1ProducerConfigs();
    List<Config> configs = new ArrayList<>();

    KafkaTargetUpgrader kafkaTargetUpgrader = new KafkaTargetUpgrader();
    kafkaTargetUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    assertEquals("responseConf.sendResponseToOrigin", configs.get(0).getName());
    assertEquals("responseConf.responseType", configs.get(1).getName());
  }

  @Test
  public void testUpgradeV4ToV5() throws Exception{
    List<Config> configs = new ArrayList<>();

    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/KafkaDTarget.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        new KafkaTargetUpgrader(),
        yamlResource
    );

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.messageKeyFormat", "STRING");
  }

  @Test
  public void testUpgradeV5ToV6() throws Exception{
    List<Config> configs = new ArrayList<>();

    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/KafkaDTarget.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        new KafkaTargetUpgrader(),
        yamlResource
    );

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(
        configs,
        "conf.dataGeneratorFormatConfig.basicAuthUserInfo",
        ""
    );
    UpgraderTestUtils.assertExists(
        configs,
        "conf.dataGeneratorFormatConfig.basicAuthUserInfoForRegistration",
        ""
    );
  }

  @Test
  public void testUpgradeV6ToV7() throws Exception {
    List<Config> configs = new ArrayList<>();

    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/KafkaDTarget.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        new KafkaTargetUpgrader(),
        yamlResource
    );

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(6).when(context).getFromVersion();
    Mockito.doReturn(7).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

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
  public void testV7toV8() {
    KafkaSecurityUpgradeHelper.testUpgradeSecurityOptions(
        SelectorStageUpgrader.createTestInstanceForStageClass(KafkaDTarget.class),
        7,
        "conf",
        "kafkaProducerConfigs",
        "metadataBrokerList"
    );
  }

  @Test
  public void testV8toV9() {
    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/KafkaDTarget.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        new KafkaTargetUpgrader(),
        yamlResource
    );
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(8).when(context).getFromVersion();
    Mockito.doReturn(9).when(context).getToVersion();

    final List<Map<String, String>> kafkaClientConfigs = new LinkedList<>();
    Map<String, String> configMap = new HashMap<>();
    configMap.put("key", "sasl.mechanism");
    configMap.put("value","PLAIN");
    kafkaClientConfigs.add(configMap);

    String stageConfigPath = "conf";
    String kafkaConfigsPath = stageConfigPath + ".kafkaProducerConfigs";
    String kafkaSecurityProtocolPath = stageConfigPath+".connectionConfig.connection.securityConfig.securityOption";
    String kafkaMechanismPath = stageConfigPath+".connectionConfig.connection.securityConfig.saslMechanism";

    configs.add(new Config(kafkaConfigsPath, Collections.unmodifiableList(kafkaClientConfigs)));
    configs.add(new Config(kafkaSecurityProtocolPath, "SASL_PLAINTEXT"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, kafkaSecurityProtocolPath, "SASL_PLAINTEXT");
    UpgraderTestUtils.assertExists(configs, kafkaMechanismPath, true);
  }

  @Test
  public void testV9toV10() {
    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/KafkaDTarget.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        new KafkaTargetUpgrader(),
        yamlResource
    );
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(9).when(context).getFromVersion();
    Mockito.doReturn(10).when(context).getToVersion();

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
