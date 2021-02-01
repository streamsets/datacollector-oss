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
package com.streamsets.pipeline.lib.kafka.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class KafkaSecurityConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Security Option",
      description = "Authentication and encryption option used to connect to the Kafka brokers",
      defaultValue = "PLAINTEXT",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(KafkaSecurityOptionsChooserValues.class)
  public KafkaSecurityOptions securityOption;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "GSSAPI",
      label = "SASL Mechanism",
      description = "SASL mechanism to use",
      displayPosition = 15,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SASL_PLAINTEXT", "SASL_SSL"})
      },
      group = "#0"
  )
  @ValueChooserModel(SaslMechanismsChooserValues.class)
  public SaslMechanisms saslMechanism;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Kerberos Service Name",
      description = "Kerberos service principal name that the Kafka brokers run as",
      defaultValue = "",
      displayPosition = 20,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SASL_PLAINTEXT", "SASL_SSL"}),
          @Dependency(configName = "saslMechanism", triggeredByValues = {"GSSAPI"})
      },
      group = "#0"
  )
  public String kerberosServiceName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Provide Keytab at Runtime",
      description = "Use a unique Kerberos keytab and principal for this stage to securely connect to Kafka through Kerberos. Overrides the default Kerberos keytab and principal configured for the Data Collector installation.",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SASL_PLAINTEXT", "SASL_SSL"}),
          @Dependency(configName = "saslMechanism", triggeredByValues = {"GSSAPI"})
      },
      group = "#0"
  )
  public boolean provideKeytab;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Runtime Keytab",
      description = "Base64 encoded keytab to use for this stage. Paste the contents of the base64 encoded keytab, or use a credential function to retrieve the base64 keytab from a credential store.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SASL_PLAINTEXT", "SASL_SSL"}),
          @Dependency(configName = "provideKeytab", triggeredByValues = {"true"})
      },
      group = "#0",
      upload = ConfigDef.Upload.BASE64
  )
  public CredentialValue userKeytab;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "user/host@REALM",
      label = "Runtime Principal",
      description = "Kerberos service principal to use for this stage.",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SASL_PLAINTEXT", "SASL_SSL"}),
          @Dependency(configName = "provideKeytab", triggeredByValues = {"true"})
      },
      group = "#0"
  )
  public String userPrincipal;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Truststore Type",
      description = "Type of the truststore",
      defaultValue = "JKS",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL", "SSL_AUTH", "SASL_SSL"})
      },
      group = "#0"
  )
  @ValueChooserModel(KeystoreTypesChooserValues.class)
  public KeystoreTypes truststoreType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Truststore File",
      description = "Absolute path to the truststore file",
      defaultValue = "",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL", "SSL_AUTH", "SASL_SSL"})
      },
      group = "#0"
  )
  public String truststoreFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Truststore Password",
      description = "Password for the truststore file",
      defaultValue = "",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL", "SSL_AUTH", "SASL_SSL"})
      },
      group = "#0"
  )
  public CredentialValue truststorePassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Keystore Type",
      description = "Type of the keystore",
      defaultValue = "JKS",
      displayPosition = 55,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL_AUTH"})
      },
      group = "#0"
  )
  @ValueChooserModel(KeystoreTypesChooserValues.class)
  public KeystoreTypes keystoreType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Keystore File",
      description = "",
      defaultValue = "",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL_AUTH"})
      },
      group = "#0"
  )
  public String keystoreFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Keystore Password",
      description = "Password for the keystore file",
      defaultValue = "",
      displayPosition = 65,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL_AUTH"})
      },
      group = "#0"
  )
  public CredentialValue keystorePassword;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Key Password",
      description = "Password for the key in the keystore",
      defaultValue = "",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL_AUTH"})
      },
      group = "#0"
  )
  public CredentialValue keyPassword;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Enabled Protocols",
      description = "Comma separated list of protocols enabled for SSL connections",
      defaultValue = "TLSv1.2",
      displayPosition = 75,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(configName = "securityOption", triggeredByValues = {"SSL", "SSL_AUTH", "SASL_SSL"})
      },
      group = "#0"
  )
  public String enabledProtocols;

}
