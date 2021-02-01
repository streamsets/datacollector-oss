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
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.kafka.connection.KafkaSecurityConfig;
import com.streamsets.pipeline.lib.kafka.connection.KafkaSecurityOptions;
import com.streamsets.pipeline.lib.kafka.connection.SaslMechanisms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaSecurityUtil {

  private static final String SECURITY_PROTOCOL = "security.protocol";

  private static final String KRB_SERVICE_NAME = "sasl.kerberos.service.name";
  private static final String SASL_MECHANISM = "sasl.mechanism";

  private static final String TRUSTSTORE_LOCATION = "ssl.truststore.location";
  private static final String TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  private static final String TRUSTSTORE_TYPE = "ssl.truststore.type";
  private static final String KEYSTORE_LOCATION = "ssl.keystore.location";
  private static final String KEYSTORE_PASSWORD = "ssl.keystore.password";
  private static final String KEY_PASSWORD = "ssl.key.password";
  private static final String KEYSTORE_TYPE = "ssl.keystore.type";
  private static final String ENABLED_PROTOCOLS = "ssl.enabled.protocols";

  public static void addSecurityConfigs(KafkaSecurityConfig securityConfig, Map<String, String> configMap) {

    // config property can be null for MapR Streams stages
    if (securityConfig.securityOption != null) {
      configMap.put(SECURITY_PROTOCOL, securityConfig.securityOption.getProtocol());

      // SSL Options
      if (securityConfig.securityOption.isOneOf(
          KafkaSecurityOptions.SSL, KafkaSecurityOptions.SSL_AUTH,
          KafkaSecurityOptions.SASL_SSL)) {
        configMap.put(ENABLED_PROTOCOLS, securityConfig.enabledProtocols);

        configMap.put(TRUSTSTORE_LOCATION, securityConfig.truststoreFile);
        configMap.put(TRUSTSTORE_PASSWORD, securityConfig.truststorePassword.get());
        configMap.put(TRUSTSTORE_TYPE, securityConfig.truststoreType.getLabel());

        if (securityConfig.securityOption.equals(KafkaSecurityOptions.SSL_AUTH)) {
          configMap.put(KEYSTORE_LOCATION, securityConfig.keystoreFile);
          configMap.put(KEYSTORE_PASSWORD, securityConfig.keystorePassword.get());
          configMap.put(KEY_PASSWORD, securityConfig.keyPassword.get());
          configMap.put(KEYSTORE_TYPE, securityConfig.keystoreType.getLabel());
        }
      }

      // Kerberos Options
      if (securityConfig.securityOption.isOneOf(KafkaSecurityOptions.SASL_PLAINTEXT, KafkaSecurityOptions.SASL_SSL)) {
        configMap.put(SASL_MECHANISM, securityConfig.saslMechanism.getMechanism());
        if (securityConfig.saslMechanism.isOneOf(SaslMechanisms.GSSAPI)) {
          configMap.put(KRB_SERVICE_NAME, securityConfig.kerberosServiceName);
        }
      }
    }
  }

  public static void validateAdditionalProperties(
      KafkaSecurityConfig securityConfig,
      Map<String, String> additionalProperties,
      String configGroupName,
      String configName,
      List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    if (securityConfig.securityOption != null) {
      List<String> forbiddenProperties = new ArrayList<>();
      forbiddenProperties.add(SECURITY_PROTOCOL);

      if (securityConfig.securityOption.isOneOf(KafkaSecurityOptions.SSL, KafkaSecurityOptions.SSL_AUTH,
          KafkaSecurityOptions.SASL_SSL)) {
        forbiddenProperties.addAll(Arrays.asList(TRUSTSTORE_TYPE, TRUSTSTORE_LOCATION, TRUSTSTORE_PASSWORD, ENABLED_PROTOCOLS));
        if (securityConfig.securityOption.equals(KafkaSecurityOptions.SSL_AUTH)) {
          forbiddenProperties.addAll(Arrays.asList(KEYSTORE_TYPE, KEYSTORE_LOCATION, KEYSTORE_PASSWORD, KEY_PASSWORD));
        }
      }

      // Kerberos Options
      if (securityConfig.securityOption.isOneOf(KafkaSecurityOptions.SASL_PLAINTEXT, KafkaSecurityOptions.SASL_SSL)) {
        forbiddenProperties.add(KRB_SERVICE_NAME);
        forbiddenProperties.add(SASL_MECHANISM);
      }

      if (!Collections.disjoint(additionalProperties.keySet(), forbiddenProperties)) {
        issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_14));
      }
    }
  }
}
