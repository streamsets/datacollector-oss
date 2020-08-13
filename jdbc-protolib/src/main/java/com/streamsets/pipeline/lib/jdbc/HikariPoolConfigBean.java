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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

public abstract class HikariPoolConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HikariPoolConfigBean.class);

  private static final String GENERIC_CONNECTION_STRING_TEMPLATE = "://%s:%d%s";

  private static final String CONF_DRIVERS_LOAD = "com.streamsets.pipeline.stage.jdbc.drivers.load";

  private static final int TEN_MINUTES = 600;
  private static final int THIRTY_MINUTES = 1800;
  private static final int THIRTY_SECONDS = 30;

  private static final String DEFAULT_CONNECTION_TIMEOUT_EL = "${30 * SECONDS}";
  private static final String DEFAULT_IDLE_TIMEOUT_EL = "${10 * MINUTES}";
  private static final String DEFAULT_MAX_LIFETIME_EL = "${30 * MINUTES}";

  private static final int MAX_POOL_SIZE_MIN = 1;
  private static final int MIN_IDLE_MIN = 0;
  private static final int CONNECTION_TIMEOUT_MIN = 1;
  private static final int IDLE_TIMEOUT_MIN = 0;
  private static final int MAX_LIFETIME_MIN = 1800;

  public static final int MILLISECONDS = 1000;
  public static final int DEFAULT_CONNECTION_TIMEOUT = THIRTY_SECONDS;
  public static final int DEFAULT_IDLE_TIMEOUT = TEN_MINUTES;
  public static final int DEFAULT_MAX_LIFETIME = THIRTY_MINUTES;
  public static final int DEFAULT_MAX_POOL_SIZE = 1;
  public static final int DEFAULT_MIN_IDLE = 1;
  public static final boolean DEFAULT_READ_ONLY = true;

  public static final String HIKARI_BEAN_NAME = "hikariConfigBean.";
  public static final String MAX_POOL_SIZE_NAME = "maximumPoolSize";
  public static final String MIN_IDLE_NAME = "minIdle";
  public static final String CONNECTION_TIMEOUT_NAME = "connectionTimeout";
  public static final String IDLE_TIMEOUT_NAME = "idleTimeout";
  public static final String MAX_LIFETIME_NAME = "maxLifetime";
  public static final String READ_ONLY_NAME = "readOnly";

  private Properties additionalProperties = new Properties();

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[]",
      label = "Additional JDBC Configuration Properties",
      description = "Additional properties to pass to the underlying JDBC driver.",
      displayPosition = 999,
      group = "JDBC"
  )
  @ListBeanModel
  public List<ConnectionPropertyBean> driverProperties = new ArrayList<>();

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.STRING,
      label = "JDBC Driver Class Name",
      description = "Class name for pre-JDBC 4 compliant drivers.",
      displayPosition = 10,
      group = "LEGACY"
  )
  public String driverClassName = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "Connection Health Test Query",
      description = "Not recommended for JDBC 4 compliant drivers. Runs when a new database connection is established.",
      displayPosition = 20,
      group = "LEGACY"
  )
  public String connectionTestQuery = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Pool Size",
      description = "Maximum number of connections to create to the data source",
      min = 1,
      defaultValue = "1",
      displayPosition = 10,
      group = "ADVANCED"
  )
  public int maximumPoolSize = DEFAULT_MAX_POOL_SIZE;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Minimum Idle Connections",
      description = "Minimum number of connections to maintain. It is recommended to set this to the same value" +
          "as Maximum Pool Size which effectively creates a fixed connection pool.",
      min = 0,
      defaultValue = "1",
      displayPosition = 20,
      group = "ADVANCED"
  )
  public int minIdle = DEFAULT_MIN_IDLE;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connection Timeout (Seconds)",
      description = "Maximum time to wait for a connection to become available. Exceeding will cause a pipeline error.",
      min = 1,
      defaultValue = DEFAULT_CONNECTION_TIMEOUT_EL,
      elDefs = {TimeEL.class},
      displayPosition = 30,
      group = "ADVANCED"
  )
  public int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Idle Timeout (Seconds)",
      description = "Maximum amount of time that a connection is allowed to sit idle in the pool. " +
          "Use 0 to opt out of an idle timeout. " +
          "If set too close to or more than Max Connection Lifetime, the property is ignored.",
      min = 0,
      defaultValue = DEFAULT_IDLE_TIMEOUT_EL,
      elDefs = {TimeEL.class},
      displayPosition = 40,
      group = "ADVANCED"
  )
  public int idleTimeout = DEFAULT_IDLE_TIMEOUT;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Connection Lifetime (Seconds)",
      description = "Maximum lifetime of a connection in the pool. When reached, the connection is retired from the pool. " +
          "Use 0 to set no maximum lifetime. When set, the minimum lifetime is 30 minutes.",
      min = 0,
      defaultValue = DEFAULT_MAX_LIFETIME_EL,
      elDefs = {TimeEL.class},
      displayPosition = 50,
      group = "ADVANCED"
  )
  public int maxLifetime = DEFAULT_MAX_LIFETIME;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Auto Commit",
      description = "Whether the connection should have property auto-commit set to true or not.",
      defaultValue = "false",
      displayPosition = 55,
      group = "ADVANCED"
  )
  public boolean autoCommit = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enforce Read-only Connection",
      description = "Should be set to true whenever possible to avoid unintended writes. Set to false with extreme " +
          "caution.",
      defaultValue = "true",
      displayPosition = 60,
      group = "ADVANCED"
  )
  public boolean readOnly = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "Init Query",
      description = "SQL query that will be executed on all new connections when they are created, before they are" +
          " added to the connection pool.",
      displayPosition = 80,
      group = "ADVANCED"
  )
  public String initialQuery = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Transaction Isolation",
      description = "Transaction isolation that should be used for all database connections.",
      defaultValue = "DEFAULT",
      displayPosition = 70,
      group = "ADVANCED"
  )
  @ValueChooserModel(TransactionIsolationLevelChooserValues.class)
  public TransactionIsolationLevel transactionIsolation = TransactionIsolationLevel.DEFAULT;

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";


  public List<Stage.ConfigIssue> validateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    ensureJdbcDrivers(context);

    // Finally we dump registered drivers to logs
    LOG.info("Currently Registered JDBC drivers:");
    Collections.list(DriverManager.getDrivers()).forEach(driver -> {
      LOG.info("Driver class {} (version {}.{})", driver.getClass().getName(), driver.getMajorVersion(), driver.getMinorVersion());
    });

    // Validation for NUMBER fields is currently disabled due to allowing ELs so we do our own here.
    if (maximumPoolSize < MAX_POOL_SIZE_MIN) {
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              MAX_POOL_SIZE_NAME,
              JdbcErrors.JDBC_10,
              maximumPoolSize,
              MAX_POOL_SIZE_NAME
          )
      );
    }

    if (minIdle < MIN_IDLE_MIN) {
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              MIN_IDLE_NAME,
              JdbcErrors.JDBC_10,
              minIdle,
              MIN_IDLE_MIN
          )
      );
    }

    if (minIdle > maximumPoolSize) {
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              MIN_IDLE_NAME,
              JdbcErrors.JDBC_11,
              minIdle,
              maximumPoolSize
          )
      );
    }

    if (connectionTimeout < CONNECTION_TIMEOUT_MIN) {
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              CONNECTION_TIMEOUT_NAME,
              JdbcErrors.JDBC_10,
              connectionTimeout,
              CONNECTION_TIMEOUT_MIN
          )
      );
    }

    if (idleTimeout < IDLE_TIMEOUT_MIN) {
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              IDLE_TIMEOUT_NAME,
              JdbcErrors.JDBC_10,
              idleTimeout,
              IDLE_TIMEOUT_MIN
          )
      );
    }

    if (maxLifetime < MAX_LIFETIME_MIN) {
      issues.add(
          context.createConfigIssue(
              Groups.ADVANCED.name(),
              MAX_LIFETIME_NAME,
              JdbcErrors.JDBC_10,
              maxLifetime,
              MAX_LIFETIME_MIN
          )
      );
    }

    if (!driverClassName.isEmpty()) {
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        issues.add(context.createConfigIssue(com.streamsets.pipeline.stage.origin.jdbc.Groups.LEGACY.name(), DRIVER_CLASSNAME, JdbcErrors.JDBC_28, e.toString()));
      }
    }

    return issues;
  }

  /**
   * Java services facility that JDBC uses for auto-loading drivers doesn't entirely work properly with multiple
   * class loaders. To ease on the user experience, we attempt to remediate the situation in various ways.
   */
  private void ensureJdbcDrivers(Stage.Context context) {
    // Currently loaded drivers
    Set<String> loadedDrivers = new HashSet<>();
    Collections.list(DriverManager.getDrivers()).forEach(driver -> loadedDrivers.add(driver.getClass().getName()));

    // 1) Attempt at improving the situation with Java not always working properly with JDBC drivers is that we
    // go over all known JDBC 4 compatible drivers again (via the services concept) and make sure that they are all
    // registered.
    LOG.debug("Exploring Service Loader for available JDBC drivers");
    for (Driver driver : ServiceLoader.load(Driver.class)) {
      if(loadedDrivers.contains(driver.getClass().getName())) {
        LOG.debug("Driver {} already loaded", driver.getClass().getName());
      } else {
        LOG.debug("Driver {} wasn't registered, registering now", driver.getClass().getName());
        try {
          DriverManager.registerDriver(driver);
          loadedDrivers.add(driver.getClass().getName());
        } catch (SQLException e) {
          LOG.error("Explicit registration of {} have failed: {}", driver.getClass().getName(), e.getMessage(), e);
        }
      }
    }

    // 2) Explicitly attempting to load known drivers if the service loading fails again
    loadVendorDriver(loadedDrivers);

    // 3) User can explicitly configure to auto-load given drivers
    LOG.debug("Loading explicitly configured drivers");
    String explicitDrivers = context.getConfiguration().get(CONF_DRIVERS_LOAD, "");
    for(String driver : explicitDrivers.split(",")) {
      ensureJdbcDriverIfNeeded(loadedDrivers, driver);
    }
  }

  protected void loadVendorDriver(Set<String> loadedDrivers) {
    LOG.debug("Loading known JDBC drivers");
    for(DatabaseVendor vendor: DatabaseVendor.values()) {
      if(vendor.getDrivers() != null) {
        for (String driver : vendor.getDrivers()) {
          ensureJdbcDriverIfNeeded(loadedDrivers, driver);
        }
      }
    }
  }

  protected void ensureJdbcDriverIfNeeded(Set<String> loadedDrivers, String driver) {
    try {
      Class klass = Class.forName(driver);

      if (loadedDrivers.contains(klass.getName())) {
        LOG.debug("Driver {} already known", driver);
      } else {
        DriverManager.registerDriver((Driver) klass.newInstance());
        loadedDrivers.add(klass.getName());
      }

    } catch (Throwable e) {
      LOG.debug("Can't pre-load {} ({})", driver, e.getClass().getSimpleName());
    }
  }

  public Properties getDriverProperties() throws StageException {
    for (ConnectionPropertyBean bean : driverProperties) {
      additionalProperties.setProperty(bean.key, bean.value.get());
    }
    return additionalProperties;
  }

  public void addExtraDriverProperties(Map<String, String> keyValueProperties) {
    for (Map.Entry<String, String> property : keyValueProperties.entrySet()) {
      additionalProperties.setProperty(property.getKey(), property.getValue());
    }
  }

  public String getConnectionStringTemplate() {
    return getConnectionString().split("://")[0].concat(GENERIC_CONNECTION_STRING_TEMPLATE);
  }

  public Set<BasicConnectionString.Pattern> getPatterns() {
    List<BasicConnectionString.Pattern> listOfPatterns = new ArrayList<>();

    // As it is the generic JDBC we want to match any connection string

    listOfPatterns.add(new BasicConnectionString.Pattern("((.)*)", 1, null, 0, 1, 0));

    return ImmutableSet.copyOf(listOfPatterns);
  }

  public ErrorCode getNonBasicUrlErrorCode() {
    return JdbcErrors.JDBC_500;
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  public boolean isAutoCommit() {
    return autoCommit;
  }

  public abstract String getConnectionString();

  public abstract DatabaseVendor getVendor();

  public abstract CredentialValue getUsername();

  public abstract CredentialValue getPassword();

  public abstract boolean useCredentials();

  public void setConnectionString(String connectionString) {
    // Do nothing, in this case since we are using the generic JDBC we don't want to change the original connection
    // string, be careful this method should not be used in test but must be used in any new change.
  }
}
