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
import com.streamsets.pipeline.lib.kafka.connection.KeystoreTypes;
import com.streamsets.pipeline.lib.kafka.connection.SaslMechanisms;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSecuriityUtil {

  private static final String KRB_SERVICE_NAME = "sasl.kerberos.service.name";
  private static final String KRB_SERVICE_NAME_TEST = "sasl.kerberos.service.name.test";
  private static final String TRUSTSTORE_LOCATION = "ssl.truststore.location";
  private static final String TRUSTSTORE_LOCATION_TEST = "ssl.truststore.location.test";
  private static final String TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  private static final String TRUSTSTORE_PASSWORD_TEST = "ssl.truststore.password.test";
  private static final String TRUSTSTORE_TYPE = "ssl.truststore.type";
  private static final KeystoreTypes TRUSTSTORE_TYPE_TEST = KeystoreTypes.JKS;
  private static final String KEYSTORE_LOCATION = "ssl.keystore.location";
  private static final String KEYSTORE_LOCATION_TEST = "ssl.keystore.location.test";
  private static final String KEYSTORE_PASSWORD = "ssl.keystore.password";
  private static final String KEYSTORE_PASSWORD_TEST = "ssl.keystore.password.test";
  private static final String KEY_PASSWORD = "ssl.key.password";
  private static final String KEY_PASSWORD_TEST = "ssl.key.password.test";
  private static final String KEYSTORE_TYPE = "ssl.keystore.type";
  private static final KeystoreTypes KEYSTORE_TYPE_TEST = KeystoreTypes.JKS;
  private static final String ENABLED_PROTOCOLS = "ssl.enabled.protocols";
  private static final String ENABLED_PROTOCOLS_TEST = "ssl.enabled.protocols.test";
  private static final String SECURITY_PROTOCOL = "security.protocol";
  private static final String SECURITY_PROTOCOL_TEST = "security.protocol.test";
  private static final String SASL_MECHANISM = "sasl.mechanism";
  private static final String GSSAPI = "GSSAPI";

  private KafkaSecurityConfig securityConfig;
  private Map<String, String> configMap;
  private Map<String, String> additionalPropeties;
  private List<Stage.ConfigIssue> issues;
  private Stage.Context context;

  @Before
  public void setUp() {
    securityConfig = new KafkaSecurityConfig();
    securityConfig.kerberosServiceName = KRB_SERVICE_NAME_TEST;
    securityConfig.truststoreFile = TRUSTSTORE_LOCATION_TEST;
    securityConfig.truststorePassword = () -> TRUSTSTORE_PASSWORD_TEST;
    securityConfig.truststoreType = KeystoreTypes.JKS;
    securityConfig.keystoreFile = KEYSTORE_LOCATION_TEST;
    securityConfig.keystorePassword = () -> KEYSTORE_PASSWORD_TEST;
    securityConfig.keyPassword = () -> KEY_PASSWORD_TEST;
    securityConfig.keystoreType = KeystoreTypes.JKS;
    securityConfig.enabledProtocols = ENABLED_PROTOCOLS_TEST;
    configMap = new HashMap<>();
    additionalPropeties = new HashMap<>();
    issues = new ArrayList<>();
    context = Mockito.mock(Stage.Context.class);
  }

  @Test
  public void testAddSecurityConfigsPlaintext() {
    securityConfig.securityOption = KafkaSecurityOptions.PLAINTEXT;
    KafkaSecurityUtil.addSecurityConfigs(securityConfig, configMap);
    Assert.assertEquals(1, configMap.size());
    Assert.assertNotNull(configMap.get(SECURITY_PROTOCOL));
    Assert.assertEquals(KafkaSecurityOptions.PLAINTEXT.getProtocol(), configMap.get(SECURITY_PROTOCOL));
  }

  @Test
  public void testAddSecurityConfigsSsl() {
    securityConfig.securityOption = KafkaSecurityOptions.SSL;
    KafkaSecurityUtil.addSecurityConfigs(securityConfig, configMap);
    Assert.assertEquals(5, configMap.size());
    Assert.assertNotNull(configMap.get(SECURITY_PROTOCOL));
    Assert.assertEquals(KafkaSecurityOptions.SSL.getProtocol(), configMap.get(SECURITY_PROTOCOL));
    Assert.assertNotNull(configMap.get(ENABLED_PROTOCOLS));
    Assert.assertEquals(configMap.get(ENABLED_PROTOCOLS), ENABLED_PROTOCOLS_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_LOCATION));
    Assert.assertEquals(configMap.get(TRUSTSTORE_LOCATION), TRUSTSTORE_LOCATION_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_PASSWORD));
    Assert.assertEquals(configMap.get(TRUSTSTORE_PASSWORD), TRUSTSTORE_PASSWORD_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_TYPE));
    Assert.assertEquals(configMap.get(TRUSTSTORE_TYPE), TRUSTSTORE_TYPE_TEST.name());
  }

  @Test
  public void testAddSecurityConfigsSslAuth() {
    securityConfig.securityOption = KafkaSecurityOptions.SSL_AUTH;
    KafkaSecurityUtil.addSecurityConfigs(securityConfig, configMap);
    Assert.assertEquals(9, configMap.size());
    Assert.assertNotNull(configMap.get(SECURITY_PROTOCOL));
    Assert.assertEquals(KafkaSecurityOptions.SSL_AUTH.getProtocol(), configMap.get(SECURITY_PROTOCOL));
    Assert.assertNotNull(configMap.get(ENABLED_PROTOCOLS));
    Assert.assertEquals(configMap.get(ENABLED_PROTOCOLS), ENABLED_PROTOCOLS_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_LOCATION));
    Assert.assertEquals(configMap.get(TRUSTSTORE_LOCATION), TRUSTSTORE_LOCATION_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_PASSWORD));
    Assert.assertEquals(configMap.get(TRUSTSTORE_PASSWORD), TRUSTSTORE_PASSWORD_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_TYPE));
    Assert.assertEquals(configMap.get(TRUSTSTORE_TYPE), TRUSTSTORE_TYPE_TEST.name());
    Assert.assertNotNull(configMap.get(KEYSTORE_LOCATION));
    Assert.assertEquals(configMap.get(KEYSTORE_LOCATION), KEYSTORE_LOCATION_TEST);
    Assert.assertNotNull(configMap.get(KEYSTORE_PASSWORD));
    Assert.assertEquals(configMap.get(KEYSTORE_PASSWORD), KEYSTORE_PASSWORD_TEST);
    Assert.assertNotNull(configMap.get(KEY_PASSWORD));
    Assert.assertEquals(configMap.get(KEY_PASSWORD), KEY_PASSWORD_TEST);
    Assert.assertNotNull(configMap.get(KEYSTORE_TYPE));
    Assert.assertEquals(configMap.get(KEYSTORE_TYPE), KEYSTORE_TYPE_TEST.name());
  }

  @Test
  public void testAddSecurityConfigsSaslPlaintext() {
    securityConfig.securityOption = KafkaSecurityOptions.SASL_PLAINTEXT;
    securityConfig.saslMechanism = SaslMechanisms.GSSAPI;
    KafkaSecurityUtil.addSecurityConfigs(securityConfig, configMap);
    Assert.assertEquals(3, configMap.size());
    Assert.assertNotNull(configMap.get(SECURITY_PROTOCOL));
    Assert.assertEquals(KafkaSecurityOptions.SASL_PLAINTEXT.getProtocol(), configMap.get(SECURITY_PROTOCOL));
    Assert.assertNotNull(configMap.get(KRB_SERVICE_NAME));
    Assert.assertEquals(KRB_SERVICE_NAME_TEST, configMap.get(KRB_SERVICE_NAME));
    Assert.assertNotNull(configMap.get(SASL_MECHANISM));
    Assert.assertEquals(GSSAPI, configMap.get(SASL_MECHANISM));
  }

  @Test
  public void testAddSecurityConfigsSaslSsl() {
    securityConfig.securityOption = KafkaSecurityOptions.SASL_SSL;
    securityConfig.saslMechanism = SaslMechanisms.GSSAPI;
    KafkaSecurityUtil.addSecurityConfigs(securityConfig, configMap);
    Assert.assertEquals(7, configMap.size());
    Assert.assertNotNull(configMap.get(SECURITY_PROTOCOL));
    Assert.assertEquals(KafkaSecurityOptions.SASL_SSL.getProtocol(), configMap.get(SECURITY_PROTOCOL));
    Assert.assertNotNull(configMap.get(ENABLED_PROTOCOLS));
    Assert.assertEquals(configMap.get(ENABLED_PROTOCOLS), ENABLED_PROTOCOLS_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_LOCATION));
    Assert.assertEquals(configMap.get(TRUSTSTORE_LOCATION), TRUSTSTORE_LOCATION_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_PASSWORD));
    Assert.assertEquals(configMap.get(TRUSTSTORE_PASSWORD), TRUSTSTORE_PASSWORD_TEST);
    Assert.assertNotNull(configMap.get(TRUSTSTORE_TYPE));
    Assert.assertEquals(configMap.get(TRUSTSTORE_TYPE), TRUSTSTORE_TYPE_TEST.name());
    Assert.assertNotNull(configMap.get(KRB_SERVICE_NAME));
    Assert.assertEquals(configMap.get(KRB_SERVICE_NAME), KRB_SERVICE_NAME_TEST);
    Assert.assertNotNull(configMap.get(SASL_MECHANISM));
    Assert.assertEquals(GSSAPI, configMap.get(SASL_MECHANISM));
  }

  @Test
  public void testAddSecurityConfigsEmptyOptions() {
    securityConfig.securityOption = null;
    KafkaSecurityUtil.addSecurityConfigs(securityConfig, configMap);
    Assert.assertEquals(0, configMap.size());
  }

  @Test
  public void testValidateAdditionalPropertiesPlaintext() {
    securityConfig.securityOption = KafkaSecurityOptions.PLAINTEXT;
    additionalPropeties.put(SECURITY_PROTOCOL, SECURITY_PROTOCOL_TEST);
    KafkaSecurityUtil.validateAdditionalProperties(
        securityConfig,
        additionalPropeties,
        "testConfigGroupName",
        "testConfigName",
        issues,
        context
    );
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testValidateAdditionalPropertiesSsl() {
    securityConfig.securityOption = KafkaSecurityOptions.SSL;
    additionalPropeties.put(SECURITY_PROTOCOL, SECURITY_PROTOCOL_TEST);
    additionalPropeties.put(ENABLED_PROTOCOLS, ENABLED_PROTOCOLS_TEST);
    additionalPropeties.put(TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD_TEST);
    additionalPropeties.put(TRUSTSTORE_TYPE, TRUSTSTORE_TYPE_TEST.name());
    additionalPropeties.put(TRUSTSTORE_LOCATION, TRUSTSTORE_LOCATION_TEST);
    KafkaSecurityUtil.validateAdditionalProperties(
        securityConfig,
        additionalPropeties,
        "testConfigGroupName",
        "testConfigName",
        issues,
        context
    );
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testValidateAdditionalPropertiesSslAuth() {
    securityConfig.securityOption = KafkaSecurityOptions.SSL_AUTH;
    additionalPropeties.put(SECURITY_PROTOCOL, SECURITY_PROTOCOL_TEST);
    additionalPropeties.put(ENABLED_PROTOCOLS, ENABLED_PROTOCOLS_TEST);
    additionalPropeties.put(TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD_TEST);
    additionalPropeties.put(TRUSTSTORE_TYPE, TRUSTSTORE_TYPE_TEST.name());
    additionalPropeties.put(TRUSTSTORE_LOCATION, TRUSTSTORE_LOCATION_TEST);
    additionalPropeties.put(KEYSTORE_TYPE, KEYSTORE_TYPE_TEST.name());
    additionalPropeties.put(KEYSTORE_LOCATION, KEYSTORE_LOCATION_TEST);
    additionalPropeties.put(KEYSTORE_PASSWORD, KEYSTORE_PASSWORD_TEST);
    additionalPropeties.put(KEY_PASSWORD, KEY_PASSWORD_TEST);
    KafkaSecurityUtil.validateAdditionalProperties(
        securityConfig,
        additionalPropeties,
        "testConfigGroupName",
        "testConfigName",
        issues,
        context
    );
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testValidateAdditionalPropertiesSaslPlaintext() {
    securityConfig.securityOption = KafkaSecurityOptions.SASL_PLAINTEXT;
    additionalPropeties.put(SECURITY_PROTOCOL, SECURITY_PROTOCOL_TEST);
    additionalPropeties.put(KRB_SERVICE_NAME, KRB_SERVICE_NAME_TEST);
    KafkaSecurityUtil.validateAdditionalProperties(
        securityConfig,
        additionalPropeties,
        "testConfigGroupName",
        "testConfigName",
        issues,
        context
    );
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testValidateAdditionalPropertiesSaslSsl() {
    securityConfig.securityOption = KafkaSecurityOptions.SASL_SSL;
    additionalPropeties.put(SECURITY_PROTOCOL, SECURITY_PROTOCOL_TEST);
    additionalPropeties.put(ENABLED_PROTOCOLS, ENABLED_PROTOCOLS_TEST);
    additionalPropeties.put(TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD_TEST);
    additionalPropeties.put(TRUSTSTORE_TYPE, TRUSTSTORE_TYPE_TEST.name());
    additionalPropeties.put(TRUSTSTORE_LOCATION, TRUSTSTORE_LOCATION_TEST);
    KafkaSecurityUtil.validateAdditionalProperties(
        securityConfig,
        additionalPropeties,
        "testConfigGroupName",
        "testConfigName",
        issues,
        context
    );
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testValidateAdditionalPropertiesEmptyOptions() {
    securityConfig.securityOption = null;
    additionalPropeties.put(SECURITY_PROTOCOL, SECURITY_PROTOCOL_TEST);
    KafkaSecurityUtil.validateAdditionalProperties(
        securityConfig,
        additionalPropeties,
        "testConfigGroupName",
        "testConfigName",
        issues,
        context
    );
    // Even if the additional properties are set, there are no security options to conflict with
    Assert.assertEquals(0, issues.size());
  }
}
