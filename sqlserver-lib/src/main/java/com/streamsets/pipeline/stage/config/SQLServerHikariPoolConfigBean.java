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
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import org.eclipse.collections.impl.factory.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class SQLServerHikariPoolConfigBean extends HikariPoolConfigBean {

  private static final Logger LOG = LoggerFactory.getLogger(SQLServerHikariPoolConfigBean.class);

  protected static final String DEFAULT_CONNECTION_STRING_PREFIX = "jdbc:sqlserver://";
  private static final String DEFAULT_CONNECTION_STRING = "jdbc:sqlserver://<HostName>:1433;DatabaseName=<DATABASE>";
  private static final String CONNECTION_STRING_TEMPLATE = "jdbc:sqlserver://%s:%d;DatabaseName=%s";

  private static final String ENCRYPTION_TAB = "ENCRYPTION";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = DEFAULT_CONNECTION_STRING,
      label = "Connection String",
      displayPosition = 10,
      group = "JDBC"
  )
  public String connectionString = DEFAULT_CONNECTION_STRING;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enable Encryption",
      displayPosition = 10,
      group = "ENCRYPTION")
  public boolean encrypt = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Trust Server Certificate",
      description = "Determines whether to trust the SSL certificate of the database server",
      displayPosition = 11,
      dependsOn = "encrypt",
      triggeredByValue = "true",
      group = "ENCRYPTION")
  public boolean trustServerCertificate = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Server Certificate PEM",
      description = "Contents of the server certificate in PEM format used to verify the server certificate",
      displayPosition = 20,
      group = "ENCRYPTION",
      dependsOn = "trustServerCertificate",
      triggeredByValue = "false")
  public String serverCertificatePem = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Verify Hostname in Connection String",
      description = "Verify that the host name specified in the connection string matches the CN in the server " +
          "certificate.",
      displayPosition = 30,
      dependsOn = "encrypt",
      triggeredByValue = "true",
      group = "ENCRYPTION")
  public boolean verifyHostnameInUrl = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Verify Alternate Hostname",
      description = "Alternate host name that must match the CN in the server certificate.",
      displayPosition = 40,
      group = "ENCRYPTION",
      dependsOn = "verifyHostnameInUrl",
      triggeredByValue = "false")
  public String verifyHostname = "";

  private Properties encryptionProperties;

  private static final Set<String> BLACKLISTED_PROPS = ImmutableSet.of("encrypt",
      "trustServerCertificate",
      "trustStore",
      "trustStorePassword",
      "hostNameInCertificate"
  );

  @VisibleForTesting
  List<Stage.ConfigIssue> superValidateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    return super.validateConfigs(context, issues);
  }

  @Override
  public List<Stage.ConfigIssue> validateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    issues = superValidateConfigs(context, issues);
    encryptionProperties = new Properties();

    if (!getConnectionString().startsWith(DEFAULT_CONNECTION_STRING_PREFIX)) {
      issues.add(context.createConfigIssue("JDBC", "hikariConfigBean.sqlServerConnectionString", Errors.SQLSERVER_00));
    }

    if (encrypt) {
      encryptionProperties.setProperty("encrypt", "true");
      encryptionProperties.setProperty("trustServerCertificate", Boolean.toString(trustServerCertificate));
      if (!trustServerCertificate) {
        try {
          KeyStoreBuilder builder = new KeyStoreBuilder().addCertificatePem(serverCertificatePem);
          KeyStoreIO.KeyStoreFile jks = KeyStoreIO.save(builder.build());
          encryptionProperties.setProperty("trustStore", jks.getPath());
          encryptionProperties.setProperty("trustStorePassword", jks.getPassword());
        } catch (StageException ex) {
          issues.add(context.createConfigIssue(ENCRYPTION_TAB,
              "hikariConfigBean.serverCertificatePem",
              ex.getErrorCode(),
              ex.getMessage()
          ));
        } catch (RuntimeException ex) {
          issues.add(context.createConfigIssue(ENCRYPTION_TAB,
              "hikariConfigBean.serverCertificatePem",
              Errors.SQLSERVER_03,
              ex.getMessage()
          ));
        }
      }

      if (!verifyHostnameInUrl) {
        encryptionProperties.setProperty("hostNameInCertificate", verifyHostname);
      } else if (context.getService(SshTunnelService.class) != null &&
          context.getService(SshTunnelService.class).isEnabled()) {
        issues.add(context.createConfigIssue(ENCRYPTION_TAB, "hikariConfigBean.sslMode", Errors.SQLSERVER_05));
      }
    }

    Set<String> blacklistedInUse = Sets.intersect(BLACKLISTED_PROPS, super.getDriverProperties().stringPropertyNames());
    if (!blacklistedInUse.isEmpty()) {
      issues.add(context.createConfigIssue("JDBC", "driverProperties", Errors.SQLSERVER_02, blacklistedInUse));
    }
    return issues;
  }

  @VisibleForTesting
  Properties getEncryptionProperties() {
    return encryptionProperties;
  }

  @Override
  public Properties getDriverProperties() throws StageException {
    Properties properties = super.getDriverProperties();
    properties.putAll(getEncryptionProperties());
    return properties;
  }

  @Override
  protected void loadVendorDriver(Set<String> loadedDrivers) {
    LOG.debug("Loading known JDBC drivers");
    for (String driver : DatabaseVendor.SQL_SERVER.getDrivers()) {
      ensureJdbcDriverIfNeeded(loadedDrivers, driver);
    }
  }

  @Override
  public String getConnectionString() {
    return this.connectionString;
  }

  @Override
  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  @Override
  public String getConnectionStringTemplate() {
    return CONNECTION_STRING_TEMPLATE;
  }

  @Override
  public Set<BasicConnectionString.Pattern> getPatterns() {
    List<BasicConnectionString.Pattern> listOfPatterns = new ArrayList<>();

    for (String pattern : BasicConnectionString.getAllRegex()) {
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s):(\\d+)(;.*)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 5, 0, 6));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s):(\\d+)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 5, 0, 0));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s)(;.*)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 0, 1433, 5));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 0, 1433, 0));
    }

    return ImmutableSet.copyOf(listOfPatterns);
  }

  @Override
  public ErrorCode getNonBasicUrlErrorCode() {
    return Errors.SQLSERVER_04;
  }
}
