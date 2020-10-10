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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.origin.ipctokafka.SdcIpcToKafkaUpgrader;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestUDPToKafkaUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/UDPToKafkaDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", new SdcIpcToKafkaUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV3toV4() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    configs.add(new Config("kafkaTargetConfig.kafkaProducerConfigs", ImmutableList.of(
        ImmutableMap.of("key", "security.protocol", "value", "SASL_PLAINTEXT"),
        ImmutableMap.of("key", "sasl.kerberos.service.name", "value", "kafka"),
        ImmutableMap.of("key", "ssl.truststore.type", "value", "JKS"),
        ImmutableMap.of("key", "ssl.truststore.location", "value", "/tmp/truststore"),
        ImmutableMap.of("key", "ssl.truststore.password", "value", "trustpwd"),
        ImmutableMap.of("key", "ssl.keystore.type", "value", "PKCS12"),
        ImmutableMap.of("key", "ssl.keystore.location", "value", "/tmp/keystore"),
        ImmutableMap.of("key", "ssl.keystore.password", "value", "keystpwd"),
        ImmutableMap.of("key", "ssl.key.password", "value", "keypwd"),
        ImmutableMap.of("key", "ssl.enabled.protocols", "value", "TlSv1.2, TLSv1.3")
    )));

    configs.add(new Config("kafkaTargetConfig.provideKeytab", true));
    configs.add(new Config("kafkaTargetConfig.userKeytab", "userKeytab"));
    configs.add(new Config("kafkaTargetConfig.userPrincipal", "sdc/sdc@CLUSTER"));

    configs.add(new Config("kafkaTargetConfig.metadataBrokerList", "localhost:9092"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.securityOption", "SASL_PLAINTEXT");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.kerberosServiceName", "kafka");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.provideKeytab", true);
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.userKeytab", "userKeytab");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.userPrincipal", "sdc/sdc@CLUSTER");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.truststoreType", "JKS");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.truststoreFile", "/tmp/truststore");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.truststorePassword", "trustpwd");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.keystoreType", "PKCS12");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.keystoreFile", "/tmp/keystore");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.keystorePassword", "keystpwd");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.keyPassword", "keypwd");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.securityConfig.enabledProtocols", "TlSv1.2, TLSv1.3");
    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.connectionConfig.connection.metadataBrokerList", "localhost:9092");

    UpgraderTestUtils.assertExists(configs, "kafkaTargetConfig.kafkaProducerConfigs", ImmutableList.of());
  }
}
