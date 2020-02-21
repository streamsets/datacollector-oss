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
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import org.eclipse.collections.impl.factory.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class MySQLHikariPoolConfigBean extends HikariPoolConfigBean {

  private static final String CONNECTION_STRING_TEMPLATE = "jdbc:mysql://%s:%d%s";

  protected static final String DEFAULT_CONNECTION_STRING_PREFIX = "jdbc:mysql://";
  protected static final String DEFAULT_CONNECTION_STRING = "jdbc:mysql://<hostName>:3306/<databaseName>";

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
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "SSL Mode",
      description = "SSL mode used to connect to the server",
      defaultValue = "REQUIRED",
      displayPosition = 20,
      group = "ENCRYPTION",
      triggeredByValue = "true")
  @ValueChooserModel(MySQLSSLModeChooserValues.class)
  public MySQLSSLMode sslMode = MySQLSSLMode.REQUIRED;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "CA Certificate PEM",
      description = "Contents of the CA certificate in PEM format used to verify the server certificate",
      displayPosition = 30,
      dependsOn = "sslMode",
      triggeredByValue = {"VERIFY_CA", "VERIFY_IDENTITY"},
      group = "ENCRYPTION")
  public String certificatePem = "";

  private Properties sslProperties;

  private static final Set<String> BLACKLISTED_PROPS = ImmutableSet.of("encrypt",
      "trustServerCertificate",
      "trustStore",
      "trustStorePassword",
      "hostNameInCertificate"
  );

  @VisibleForTesting
  Properties getSslProperties() {
    return sslProperties;
  }

  @VisibleForTesting
  List<Stage.ConfigIssue> superValidateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    return super.validateConfigs(context, issues);
  }

  @Override
  public List<Stage.ConfigIssue> validateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    issues = superValidateConfigs(context, issues);
    sslProperties = new Properties();
    if (!getConnectionString().startsWith(DEFAULT_CONNECTION_STRING_PREFIX)) {
      issues.add(context.createConfigIssue("JDBC", "hikariConfigBean.connectionString", Errors.MYSQL_00));
    }

    sslProperties.setProperty("sslMode", sslMode.toString());

    if (sslMode.equals(MySQLSSLMode.VERIFY_CA) || sslMode.equals(MySQLSSLMode.VERIFY_IDENTITY)) {
      try {
        KeyStoreBuilder builder = new KeyStoreBuilder().addCertificatePem(certificatePem);
        KeyStoreIO.KeyStoreFile jks = KeyStoreIO.save(builder.build());
        sslProperties.setProperty("trustCertificateKeyStoreUrl", "file://" + jks.getPath());
        sslProperties.setProperty("trustCertificateKeyStorePassword", jks.getPassword());
      } catch (StageException ex) {
        issues.add(context.createConfigIssue("ENCRYPTION",
            "hikariConfigBean.certificatePem",
            ex.getErrorCode(),
            ex.getMessage()
        ));
      } catch (RuntimeException ex) {
        issues.add(context.createConfigIssue("ENCRYPTION",
            "hikariConfigBean.certificatePem",
            Errors.MYSQL_03,
            ex.getMessage()
        ));
      }
    }
    Set<String> blacklistedInUse = Sets.intersect(BLACKLISTED_PROPS, super.getDriverProperties().stringPropertyNames());
    if (!blacklistedInUse.isEmpty()) {
      issues.add(context.createConfigIssue("JDBC", "driverProperties", Errors.MYSQL_02, blacklistedInUse));
    }
    return issues;
  }

  @Override
  public Properties getDriverProperties() throws StageException {
    Properties properties = super.getDriverProperties();
    properties.putAll(sslProperties);
    return properties;
  }

  @Override
  public String getConnectionString() {
    return connectionString;
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
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s):(\\d+)(/.*)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 5, 0, 6));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s):(\\d+)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 5, 0, 0));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s)(/.*)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 0, 3306, 5));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 0, 3306, 0));
    }
    return ImmutableSet.copyOf(listOfPatterns);
  }

  @Override
  public ErrorCode getNonBasicUrlErrorCode() {
    return Errors.MYSQL_04;
  }

  @Override
  public DatabaseVendor getVendor() {
    return DatabaseVendor.MYSQL;
  }

}
