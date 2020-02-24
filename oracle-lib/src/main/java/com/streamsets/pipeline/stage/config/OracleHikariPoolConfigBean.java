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

package com.streamsets.pipeline.stage.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.security.KeyStoreBuilder;
import com.streamsets.datacollector.security.KeyStoreIO;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import org.eclipse.collections.impl.factory.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class OracleHikariPoolConfigBean extends HikariPoolConfigBean {

  private static final String TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";
  private static final String TRUSTSTORE = "javax.net.ssl.trustStore";
  private static final String TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  private static final String SSL_CLIENT_AUTHENTICATION = "oracle.net.ssl_client_authentication";
  private static final String SSL_CIPHER_SUITES = "oracle.net.ssl_cipher_suites";
  private static final String SSL_SERVER_DN_MATCH = "oracle.net.ssl_server_dn_match";

  private static final String
      CONNECTION_SCHEME
      = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=%s) (HOST=%s) (PORT=%d)) (CONNECT_DATA= (SID=%s)) %s)";

  private static final String CONNECTION_SCHEME_DISTINGUISHED_NAME = "(SECURITY=(SSL_SERVER_CERT_DN=%s))";

  private static final String PROTOCOL = "tcp";
  private static final String SECURE_PROTOCOL = "tcps";

  private static final String CONNECTION_STRING_PROTOCOL = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=";
  private static final String CONNECTION_STRING_BASIC = ") (HOST=%s) (PORT=%d)) %s";

  private static final String
      DEFAULT_CONNECTION_REGEX
      = "jdbc\\:oracle\\:thin\\:\\@\\(DESCRIPTION\\=\\(ADDRESS\\=\\(PROTOCOL\\=%s\\) \\(HOST\\=";

  private String connectionString = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Database Host",
      description = "Host name of the machine where the database server is installed",
      displayPosition = 11,
      group = "JDBC")
  public String host = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Database Secure Port",
      description = "Secure port of the database server",
      defaultValue = "2484",
      displayPosition = 12,
      group = "JDBC")
  public Integer port;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Database SID",
      description = "Name of the database on the server, also known as the service name or SID",
      displayPosition = 13,
      group = "JDBC")
  public String ssid = "";

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Encrypt Connection",
      description = "Encrypt the connection using SSL",
      displayPosition = 10,
      group = "ENCRYPTION")
  public boolean isEncryptedConnection = true;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Server Certificate PEM",
      description = "Contents of the server certificate in PEM format used to verify the server certificate",
      displayPosition = 20,
      group = "ENCRYPTION",
      dependencies = @Dependency(configName = "isEncryptedConnection",
          triggeredByValues = "true"))
  public String serverCertificatePem = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cipher Suites",
      description = "Supported cipher suites separated by commas",
      displayPosition = 31,
      group = "ENCRYPTION",
      dependencies = @Dependency(configName = "isEncryptedConnection",
          triggeredByValues = "true"))
  public String cipherSuites = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Verify Hostname ",
      description = "Verify that the host name specified in the connection string matches the CN in the server " +
          "certificate",
      displayPosition = 32,
      group = "ENCRYPTION",
      dependencies = @Dependency(configName = "isEncryptedConnection",
          triggeredByValues = "true"))
  public boolean verifyHostname = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "SSL Distinguished Name",
      description = "Distinguished name (DN) of the server that must match the DN in the server certificate",
      displayPosition = 33,
      group = "ENCRYPTION",
      dependsOn = "verifyHostname",
      triggeredByValue = "true")
  public String distinguishedName = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 110,
      group = "CREDENTIALS"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 120,
      group = "CREDENTIALS"
  )
  public CredentialValue password;

  @Override
  public CredentialValue getUsername() {
    return username;
  }

  @Override
  public CredentialValue getPassword() {
    return password;
  }

  private Properties encryptionProperties;

  private static final Set<String> BLACKLISTED_PROPS = ImmutableSet.of(
      TRUSTSTORE_TYPE, TRUSTSTORE, TRUSTSTORE_PASSWORD,
      SSL_CLIENT_AUTHENTICATION,
      SSL_SERVER_DN_MATCH
  );

  @VisibleForTesting
  Properties getEncryptionProperties() {
    return encryptionProperties;
  }

  @Override
  public Properties getDriverProperties() throws StageException {
    Properties properties = super.getDriverProperties();
    if (isEncryptedConnection) {
      properties.putAll(getEncryptionProperties());
    }
    return properties;
  }

  @VisibleForTesting
  List<Stage.ConfigIssue> superValidateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    return super.validateConfigs(context, issues);
  }

  @Override
  public List<Stage.ConfigIssue> validateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    issues = superValidateConfigs(context, issues);

    try {
      if (isEncryptedConnection) {
        encryptionProperties = new Properties();
        KeyStoreBuilder builder = new KeyStoreBuilder().addCertificatePem(serverCertificatePem);
        KeyStoreIO.KeyStoreFile jks = KeyStoreIO.save(builder.build());
        encryptionProperties.setProperty(TRUSTSTORE_TYPE, "JKS");
        encryptionProperties.setProperty(TRUSTSTORE, jks.getPath());
        encryptionProperties.setProperty(TRUSTSTORE_PASSWORD, jks.getPassword());
        encryptionProperties.setProperty(SSL_CLIENT_AUTHENTICATION, "false");

        encryptionProperties.setProperty(SSL_CIPHER_SUITES, cipherSuites.trim());
        encryptionProperties.setProperty(SSL_SERVER_DN_MATCH, String.valueOf(verifyHostname));
      }

      constructConnectionString();

    } catch (StageException e) {
      issues.add(context.createConfigIssue("ENCRYPTION",
          "hikariConfigBean.serverCertificatePem",
          Errors.ORACLE_01,
          e.getMessage()
      ));
    }
    Set<String> blacklistedInUse = Sets.intersect(BLACKLISTED_PROPS, super.getDriverProperties().stringPropertyNames());
    if (!blacklistedInUse.isEmpty()) {
      issues.add(context.createConfigIssue("JDBC", "driverProperties", Errors.ORACLE_00, blacklistedInUse));
    }
    return issues;
  }

  @VisibleForTesting
  public void constructConnectionString() {
    connectionString = String.format(CONNECTION_SCHEME,
        getProtocol(),
        host,
        port,
        ssid,
        !(isEncryptedConnection && verifyHostname) ? "" : String.format(CONNECTION_SCHEME_DISTINGUISHED_NAME,
            distinguishedName)
    );
  }

  @Override
  public String getConnectionString() {
    if (connectionString.isEmpty()){
      constructConnectionString();
    }
    return connectionString;
  }

  @Override
  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  @Override
  public String getConnectionStringTemplate() {
    return CONNECTION_STRING_PROTOCOL.concat(getProtocol()).concat(CONNECTION_STRING_BASIC);
  }

  @Override
  public Set<BasicConnectionString.Pattern> getPatterns() {
    List<BasicConnectionString.Pattern> listOfPatterns = new ArrayList<>();

    for (String pattern : BasicConnectionString.getAllRegex()) {
      listOfPatterns.add(new BasicConnectionString.Pattern(
          String.format("%s(%s)\\) \\(PORT\\=(\\d+)\\)\\) ((.*)(\\)))*",
              String.format(DEFAULT_CONNECTION_REGEX, getProtocol()),
              pattern
          ), 1, null, 5, 0, 6));
    }

    return ImmutableSet.copyOf(listOfPatterns);
  }

  private String getProtocol() {
    return isEncryptedConnection ? SECURE_PROTOCOL : PROTOCOL;
  }

  @Override
  public DatabaseVendor getVendor() {
    return DatabaseVendor.ORACLE;
  }
}