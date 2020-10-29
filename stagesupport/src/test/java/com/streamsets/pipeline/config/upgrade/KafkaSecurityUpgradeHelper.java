/*
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.config.upgrade;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KafkaSecurityUpgradeHelper {

  public static void upgradeKafkaSecurityConfigsAndAssert(
      StageUpgrader upgrader,
      int fromVersion,
      final String stageConfigPath,
      final String kafkaConfigsPropertyName,
      final String brokerUriConfigName,
      final String keystoreFile,
      final String consumerConfigSecurityProtocol,
      final String expectedSecurityOption
  ) {

    final StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(fromVersion).when(context).getFromVersion();
    Mockito.doReturn(fromVersion+1).when(context).getToVersion();

    final List<Config> configs = new LinkedList<>();
    final List<Map<String, String>> kafkaClientConfigs = new LinkedList<>();
    kafkaClientConfigs.add(createKeyValueConfigMap("security.protocol", consumerConfigSecurityProtocol));
    kafkaClientConfigs.add(createKeyValueConfigMap("sasl.kerberos.service.name", "kafka"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.truststore.type", "JKS"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.truststore.location", "/tmp/truststore"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.truststore.password", "trustpwd"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.keystore.type", "PKCS12"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.keystore.location", keystoreFile));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.keystore.password", "keystpwd"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.key.password", "keypwd"));
    kafkaClientConfigs.add(createKeyValueConfigMap("ssl.enabled.protocols", "TlSv1.2, TLSv1.3"));

    final String kafkaConfigsPath = stageConfigPath + "." + kafkaConfigsPropertyName;
    configs.add(new Config(kafkaConfigsPath, Collections.unmodifiableList(kafkaClientConfigs)));

    configs.add(new Config(stageConfigPath+".provideKeytab", true));
    configs.add(new Config(stageConfigPath+".userKeytab", "userKeytab"));
    configs.add(new Config(stageConfigPath+".userPrincipal", "sdc/sdc@CLUSTER"));

    configs.add(new Config(stageConfigPath+"."+brokerUriConfigName, "localhost:9092"));

    final List<Config> upgradedConfigs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.securityOption", expectedSecurityOption);
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.kerberosServiceName", "kafka");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.provideKeytab", true);
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.userKeytab", "userKeytab");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.userPrincipal", "sdc/sdc@CLUSTER");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.truststoreType", "JKS");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.truststoreFile", "/tmp/truststore");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.truststorePassword", "trustpwd");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.keystoreType", "PKCS12");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.keystoreFile", keystoreFile);
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.keystorePassword", "keystpwd");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.keyPassword", "keypwd");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.securityConfig.enabledProtocols", "TlSv1.2, TLSv1.3");
    UpgraderTestUtils.assertExists(upgradedConfigs, stageConfigPath+".connectionConfig.connection.metadataBrokerList", "localhost:9092");

    // all of these should have been removed
    UpgraderTestUtils.assertExists(upgradedConfigs, kafkaConfigsPath, Collections.emptyList());
  }

  public static void testUpgradeSecurityOptions(
      StageUpgrader upgrader,
      int fromVersion,
      final String stageConfigPath,
      final String kafkaConfigsPropertyName,
      final String brokerUriConfigName
  ) {
    final String[][] cases = new String[][] {
        // if the keystore is empty, we should end up with SSL
        new String[] {"", "SSL", "SSL"},
        // if the keystore is not empty, we should end up with SSL_AUTH
        new String[] {"/tmp/keystore", "SSL", "SSL_AUTH"},
        // if the keystore is not empty, but the protocol is something besides SSL, it should be preserved
        new String[] {"/tmp/keystore", "SASL_PLAINTEXT", "SASL_PLAINTEXT"},
        // if the protocol is blank, we should default to PLAINTEXT
        new String[] {"", "", "PLAINTEXT"}
    };
    for (String[] testCase : cases) {
      final String keystore = testCase[0];
      final String consumerConfigSecurityProtocol = testCase[1];
      final String expectedSecurityOption = testCase[2];

      upgradeKafkaSecurityConfigsAndAssert(
          upgrader,
          fromVersion,
          stageConfigPath,
          kafkaConfigsPropertyName,
          brokerUriConfigName,
          keystore,
          consumerConfigSecurityProtocol,
          expectedSecurityOption
      );
    }
  }

  public static Map<String, String> createKeyValueConfigMap(final String key, final String value) {
    final Map<String, String> returnMap = new HashMap<>();
    returnMap.put("key", key);
    returnMap.put("value", value);
    return Collections.unmodifiableMap(returnMap);
  }
}
