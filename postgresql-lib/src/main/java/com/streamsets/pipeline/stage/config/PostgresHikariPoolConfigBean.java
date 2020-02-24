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
import com.streamsets.datacollector.io.TempFile;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import org.eclipse.collections.impl.factory.Sets;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class PostgresHikariPoolConfigBean extends HikariPoolConfigBean {

  static final String DEFAULT_CONNECTION_STRING_PREFIX = "jdbc:postgresql://";
  static final String DEFAULT_CONNECTION_STRING = "jdbc:postgresql://<hostName>:5432/<databaseName>";

  private static final String CONNECTION_STRING_TEMPLATE = "jdbc:postgresql://%s:%d%s";
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
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "SSL Mode",
      defaultValue = "REQUIRED",
      description = "SSL mode used to connect to the server",
      displayPosition = 20,
      group = "ENCRYPTION",
      triggeredByValue = "true")
  @ValueChooserModel(PostgresSSLModeChooserValues.class)
  public PostgresSSLMode sslMode = PostgresSSLMode.REQUIRED;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Server Certificate PEM",
      description = "Contents of the server certificate in PEM format used to verify the server certificate",
      displayPosition = 30,
      group = "ENCRYPTION",
      dependsOn = "sslMode",
      triggeredByValue = { "VERIFY_CA", "VERIFY_FULL" }
  )
  public String serverPem = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "CA Certificate PEM",
      description = "Contents of the CA certificate in PEM format used to verify the server certificate",
      displayPosition = 40,
      group = "ENCRYPTION",
      dependsOn = "sslMode",
      triggeredByValue = { "VERIFY_CA", "VERIFY_FULL" }
  )
  public String caPem = "";

  private Properties encryptionProperties;

  private static final Set<String> BLACKLISTED_PROPS = ImmutableSet.of(
      "sslmode",
      "sslcert",
      "sslrootcert"
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
      issues.add(context.createConfigIssue(
          "JDBC",
          "hikariConfigBean.postgresqlConnectionString",
          Errors.POSTGRES_00
      ));
    }

    encryptionProperties.setProperty("sslmode", sslMode.getMode());
    switch (sslMode) {
      case DISABLED:
      case REQUIRED:
        break;
      case VERIFY_CA:
      case VERIFY_FULL:
        if (sslMode.equals(PostgresSSLMode.VERIFY_FULL) &&
            context.getService(SshTunnelService.class) != null &&
            context.getService(SshTunnelService.class).isEnabled()) {
          issues.add(context.createConfigIssue(ENCRYPTION_TAB, "hikariConfigBean.sslMode", Errors.POSTGRES_06));
        } else {
          try {
            encryptionProperties.setProperty("sslcert", persistPem(serverPem));
          } catch (StageException ex) {
            issues.add(context.createConfigIssue(ENCRYPTION_TAB, "hikariConfigBean.serverPem", ex.getErrorCode()));
          }
          try {
            encryptionProperties.setProperty("sslrootcert", persistPem(caPem));
          } catch (StageException ex) {
            issues.add(context.createConfigIssue(ENCRYPTION_TAB, "hikariConfigBean.caPem", ex.getErrorCode()));
          }
          break;
        }
    }
    Set<String> blacklistedInUse = Sets.intersect(BLACKLISTED_PROPS, super.getDriverProperties().stringPropertyNames());
    if (!blacklistedInUse.isEmpty()) {
      issues.add(context.createConfigIssue("JDBC", "driverProperties", Errors.POSTGRES_02, blacklistedInUse));
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
  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  @Override
  public String getConnectionStringTemplate() {
    return CONNECTION_STRING_TEMPLATE;
  }

  protected String persistPem(String pem) {
    validatePem(pem);
    try {
      File pemFile = TempFile.createFile(".pem");
      try (Writer writer = new FileWriter(pemFile)) {
        writer.write(pem);
      }
      return pemFile.getAbsolutePath();
    } catch (IOException ex) {
      throw new StageException(Errors.POSTGRES_03, ex.getMessage());
    }
  }

  protected void validatePem(String pem) {
    pem = pem.trim();
    if (!(pem.startsWith("-----BEGIN CERTIFICATE-----") && pem.endsWith("-----END CERTIFICATE-----"))) {
      throw new StageException(Errors.POSTGRES_04);
    }
    String b64 = pem.replace("-----BEGIN CERTIFICATE-----", "").replace("-----END CERTIFICATE-----", "");
    try {
      Base64.getMimeDecoder().decode(b64);
    } catch (Exception ex) {
      throw new StageException(com.streamsets.datacollector.security.Errors.SECURITY_001, ex);
    }
  }

  @Override
  public String getConnectionString() {
    return connectionString;
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
      ), 1, null, 0, 5432, 5));
      listOfPatterns.add(new BasicConnectionString.Pattern(String.format("%s(%s)*",
          DEFAULT_CONNECTION_STRING_PREFIX,
          pattern
      ), 1, null, 0, 5432, 0));
    }
    return ImmutableSet.copyOf(listOfPatterns);
  }

  @Override
  public ErrorCode getNonBasicUrlErrorCode() {
    return Errors.POSTGRES_05;
  }
}
