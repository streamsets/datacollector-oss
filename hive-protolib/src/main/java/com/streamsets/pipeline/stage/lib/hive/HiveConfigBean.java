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
package com.streamsets.pipeline.stage.lib.hive;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HiveConfigBean.class);
  private static final String KERBEROS_JDBC_REGEX = "jdbc:.*;principal=.*@.*";
  private static final String HIVE_JDBC_URL = "hiveJDBCUrl";
  private static final String APACHE_HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  private static final String CLOUDERA_HIVE_JDBC_DRIVER = "com.cloudera.hive.jdbc4.HS2Driver";
  private static final String HIVE_PROXY_USER_KEY = "hive.server2.proxy.user";
  private static final String DELEGATION_UID_KEY = "DelegationUID";
  private static final String PROPERTY_SEPARATOR = ";";
  private static final String PROPERTY_KEY_VALUE_SEPARATOR = "=";
  private static final String IMPERSONATE_CURRENT_USER_KEY = "com.streamsets.pipeline.stage.hive.impersonate.current.user";


  @ConfigDef(
      required = true,
      label = "JDBC URL",
      type = ConfigDef.Type.STRING,
      description = "JDBC URL used to connect to Hive." +
          "Use a valid JDBC URL format, such as: jdbc:hive2://<host>:<port>/<dbname>.",
      defaultValue = "jdbc:hive2://<host>:<port>/default",
      displayPosition= 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "HIVE"
  )
  public String hiveJDBCUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "org.apache.hive.jdbc.HiveDriver",
      label = "JDBC Driver Name",
      description = "The fully-qualified JDBC driver class name",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HIVE"
  )
  public String hiveJDBCDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Credentials",
      displayPosition = 21,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HIVE"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Username",
      displayPosition = 22,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HIVE"
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Password",
      displayPosition = 23,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HIVE"
  )
  public CredentialValue password;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[]",
      label = "Additional JDBC Configuration Properties",
      description = "Additional properties to pass to the underlying JDBC driver.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HIVE"
  )
  @ListBeanModel
  public List<ConnectionPropertyBean> driverProperties = new ArrayList<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "/etc/hive/conf",
      label = "Hadoop Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load core-site.xml," +
          " hdfs-site.xml and hive-site.xml files.",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HIVE"
  )
  public String confDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional Hadoop Configuration",
      description = "Additional configuration properties. Values here override values loaded from config files.",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HIVE"
  )
  public Map<String, String> additionalConfigProperties;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Max Cache Size (entries)",
      description = "Configures the cache size for storing table related information." +
          " Use -1 for unlimited number of table entries in the cache.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public long maxCacheSize = -1L;

  private static final Joiner JOINER = Joiner.on(".");

  /**
   * After init() it will contain merged configuration from all configured sources.
   */
  private Configuration configuration;
  private UserGroupInformation loginUgi;
  private Connection hiveConnection;
  private HiveConf hConf;

  public Configuration getConfiguration() {
    return configuration;
  }
  public String getHiveConfigValue(String name) {
    return hConf.get(name);
  }

  /**
   * Returns valid connection to Hive or throws StageException.
   */
  public Connection getHiveConnection() throws StageException {
    if(!HiveMetastoreUtil.isHiveConnectionValid(hiveConnection, loginUgi)) {
      LOG.info("Connection to Hive become stale, reconnecting.");
      if(hiveConnection != null) {
        try {
          hiveConnection.close();
          LOG.info("Closed stale connection");
        } catch(SQLException ex) {
          LOG.info("Error closing stale connection {}", ex.getMessage(), ex);
        }
      }
      hiveConnection = HiveMetastoreUtil.getHiveConnection(hiveJDBCUrl, loginUgi, driverProperties);
    }
    return hiveConnection;
  }

  /**
   * This is for testing purpose
   * @param config: Configuration to set
   */
  public void setConfiguration(Configuration config) {
    configuration = config;
  }
  // This is for testing purpose.
  public void setHiveConf(HiveConf hConfig) {
    this.hConf = hConfig;
  }

  public UserGroupInformation getUgi() {
    return loginUgi;
  }

  /**
   * Initialize and validate configuration options.
   */
  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    // Load JDBC driver
    try {
      Class.forName(hiveJDBCDriver);
    } catch (ClassNotFoundException e) {
      issues.add(context.createConfigIssue(
          "HIVE",
          JOINER.join(prefix, "hiveJDBCDriver"),
          Errors.HIVE_15,
          hiveJDBCDriver
      ));
    }

    // Prepare configuration object
    File hiveConfDir = new File(confDir);
    if (!hiveConfDir.isAbsolute()) {
      hiveConfDir = new File(context.getResourcesDirectory(), confDir).getAbsoluteFile();
    }

    configuration = new Configuration();

    if (hiveConfDir.exists()) {
      HiveMetastoreUtil.validateConfigFile("core-site.xml", confDir, hiveConfDir, issues, configuration, context);
      HiveMetastoreUtil.validateConfigFile("hdfs-site.xml", confDir, hiveConfDir, issues, configuration, context);
      HiveMetastoreUtil.validateConfigFile("hive-site.xml", confDir, hiveConfDir, issues, configuration, context);

      hConf = new HiveConf(configuration, HiveConf.class);
      File confFile = new File(hiveConfDir.getAbsolutePath(), "hive-site.xml");
      hConf.addResource(new Path(confFile.getAbsolutePath()));
    } else {
      issues.add(context.createConfigIssue(
          "HIVE",
          JOINER.join(prefix, "confDir"),
          Errors.HIVE_07,
          confDir
      ));
    }

    // Add any additional configuration overrides
    for (Map.Entry<String, String> entry : additionalConfigProperties.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    if(!issues.isEmpty()) {
      return;
    }

    try {
      loginUgi = HadoopSecurityUtil.getLoginUser(configuration);
    } catch (Exception e) {
      LOG.error("Failed to connect to Hive with JDBC URL:" + hiveJDBCUrl, e);
      issues.add(
          context.createConfigIssue(
              "HIVE",
              JOINER.join(prefix, HIVE_JDBC_URL),
              Errors.HIVE_22,
              hiveJDBCUrl,
              e.getMessage()
          )
      );
      return;
    }
    try {

      if(!validateHiveImpersonation(context, prefix, issues)){
        return;
      }

      if (hiveJDBCUrl.matches(KERBEROS_JDBC_REGEX)) {
        LOG.info("Authentication: Kerberos");
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(
              context.createConfigIssue(
                  "ADVANCED",
                  JOINER.join(prefix, HIVE_JDBC_URL),
                  Errors.HIVE_38,
                  loginUgi.getAuthenticationMethod(),
                  UserGroupInformation.AuthenticationMethod.KERBEROS
              )
          );
        }
      } else {
        LOG.info("Authentication: Simple");
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
            UserGroupInformation.AuthenticationMethod.SIMPLE.name());
      }
    } catch (Exception ex) {
      LOG.info("Validation Error: " + ex.toString(), ex);
      issues.add(
          context.createConfigIssue(
              "ADVANCED",
              JOINER.join(prefix, HIVE_JDBC_URL),
              Errors.HIVE_01,
              "Exception in configuring HDFS"
          )
      );
      return;
    }

    try {
      hiveConnection = getHiveConnection();
    } catch(Exception e) {
      LOG.error(Utils.format("Error Connecting to Hive Database with URL {}", hiveJDBCUrl), e);
      issues.add(context.createConfigIssue(
          "HIVE",
          JOINER.join(prefix, HIVE_JDBC_URL),
          Errors.HIVE_22,
          jdbcUrlSafeForUser(),
          e.getMessage()
      ));
    }

    // If user wants to enter credentials we will propagate them to the properties - which is "official" way of JDBC
    // spec. The method call Driver.getDriver(url, username, password) does exactly the same for example.
    if(useCredentials) {
      ConnectionPropertyBean userBean = new ConnectionPropertyBean();
      userBean.property = "user";
      userBean.value = this.username;
      driverProperties.add(userBean);

      ConnectionPropertyBean passwordBean = new ConnectionPropertyBean();
      userBean.property = "password";
      userBean.value = this.password;
      driverProperties.add(passwordBean);
    }
  }

  private Map<String, String> getDriverNamesToProperty(){
    Map<String, String> driverNameToProperty = new HashMap<>();

    driverNameToProperty.put(APACHE_HIVE_JDBC_DRIVER, HIVE_PROXY_USER_KEY);
    driverNameToProperty.put(CLOUDERA_HIVE_JDBC_DRIVER, DELEGATION_UID_KEY);
    return driverNameToProperty;
  }

  public String jdbcUrlSafeForUser() {
    // Return all before ";" - all sensitive information is in the params afterwards
    return hiveJDBCUrl.split(";")[0];
  }

  public void destroy() {
    if (hiveConnection != null) {
      try {
        hiveConnection.close();
      } catch (Exception e) {
        LOG.error("Error with closing Hive Connection", e);
      }
    }
  }

  boolean validateHiveImpersonation(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {

    boolean useCurrentUser = Boolean.valueOf(context.getConfiguration().get(IMPERSONATE_CURRENT_USER_KEY,
        "false"));

    if(useCurrentUser){
      if (hiveJDBCUrl.contains(HIVE_PROXY_USER_KEY) || hiveJDBCUrl.contains(DELEGATION_UID_KEY)) {
        LOG.error("Current user impersonation is enabled. Hive proxy user property in the JDBC URL {} is not required"
            , hiveJDBCUrl);
        issues.add(context.createConfigIssue("HIVE", JOINER.join(prefix, HIVE_JDBC_URL), Errors.HIVE_42,
            hiveJDBCUrl));
        return false;
      }

      hiveJDBCUrl += PROPERTY_SEPARATOR + getDriverNamesToProperty().getOrDefault(hiveJDBCDriver,HIVE_PROXY_USER_KEY)
          + PROPERTY_KEY_VALUE_SEPARATOR + context.getUserContext().getAliasName();

    }
    return true;
  }
}
