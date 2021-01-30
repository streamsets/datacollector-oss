/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.connection.MySQLConnection;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filters;
import com.streamsets.pipeline.stage.origin.mysql.filters.IgnoreTableFilter;
import com.streamsets.pipeline.stage.origin.mysql.filters.IncludeTableFilter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class DataSourceInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

  private static final String PROTO_PREFIX = "jdbc:";
  private static final int MYSQL_DEFAULT_PORT = 3306;
  private static final List<String> MYSQL_DRIVERS  = ImmutableList.of(
      "com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver"
  );

  public final DataSourceConfig dataSourceConfig;
  public final HikariDataSource dataSource;
  public final Filter eventFilter;
  public final SourceOffsetFactory offsetFactory;
  public final List<Stage.ConfigIssue> issues;

  public DataSourceInitializer(
      final String connectionPrefix,
      final MySQLConnection connection,
      final String configPrefix,
      final MySQLBinLogConfig config,
      final ConfigIssueFactory configIssueFactory
  ) {
    List<ConfigIssue> issues = new ArrayList<>();

    this.dataSourceConfig = createDataSourceConfig(configPrefix, connection, config, configIssueFactory, issues);

    checkConnection(connectionPrefix, config, configIssueFactory, issues);

    eventFilter = createEventFilter(configPrefix, config, configIssueFactory, issues);

    loadDrivers();

    // connect to mysql
    dataSource = createDataSource(configIssueFactory, issues);
    offsetFactory = createOffsetFactory(configIssueFactory, issues);

    this.issues = Collections.unmodifiableList(issues);
  }

  private DataSourceConfig createDataSourceConfig(
      final String configPrefix,
      final MySQLConnection connection,
      final MySQLBinLogConfig config,
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    DataSourceConfig dsc = new DataSourceConfig(null, null, null, 0, false, 0);

    URI url = createURI(configPrefix, connection, configIssueFactory, issues);
    if (url != null) {
      dsc = new DataSourceConfig(
          url.getHost(),
          connection.username.get(),
          connection.password.get(),
          url.getPort() == -1 ? MYSQL_DEFAULT_PORT : url.getPort(),
          isSSLEnabled(url.getQuery()),
          getServerId(config)
      );
    }

    return dsc;
  }

  private URI createURI(
      final String configPrefix,
      final MySQLConnection connection,
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    URI url = null;
    if (connection.connectionString.startsWith(PROTO_PREFIX)) {
      try {
        url = new URI(connection.connectionString.substring(PROTO_PREFIX.length()));
      } catch (final URISyntaxException ex) {
        issues.add(configIssueFactory.create(
            MySQLBinLogConnectionGroups.MYSQL.name(), configPrefix + "connectionString", Errors.MYSQL_011, ex.getMessage(), ex
        ));
      }
    } else {
      issues.add(configIssueFactory.create(
          MySQLBinLogConnectionGroups.MYSQL.name(), configPrefix + "connectionString", Errors.MYSQL_011, connection.connectionString
      ));
    }
    return url;
  }

  private boolean isSSLEnabled(final String query) {
    String q = Optional.ofNullable(query).orElse("");
    q = q.startsWith("?") ? q.substring(1) : q;
    return Arrays.stream(q.split("&", -1))
        .anyMatch(p -> {
          String s = p.toLowerCase();
          return s.equals("usessl") || s.equals("requiressl")
              || s.equals("usessl=true") || s.equals("requiressl=true");
        });
  }

  private Integer getServerId(final MySQLBinLogConfig config) {
    Integer result = null;
    try {
      if (config.serverId != null && !config.serverId.isEmpty()) {
        result = Integer.valueOf(config.serverId);
      }
    } catch (final NumberFormatException e) {
      throw new NumberFormatException("Server ID must be numeric");
    }
    return result;
  }

  private Filter createEventFilter(
      final String configPrefix,
      final MySQLBinLogConfig config,
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    Filter result = null;

    // create include/ignore filters
    Filter includeFilter = createIncludeFilter(configPrefix, config, configIssueFactory, issues);
    if (includeFilter != null) {
      Filter ignoreFilter = createIgnoreFilter(configPrefix, config, configIssueFactory, issues);
      if (ignoreFilter != null) {
        result = includeFilter.and(ignoreFilter);
      }
    }

    return result;
  }

  private Filter createIgnoreFilter(
      final String configPrefix,
      final MySQLBinLogConfig config,
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    Filter ignoreFilter = null;
    try {
      Filter filter = Filters.PASS;
      if (config.ignoreTables != null && !config.ignoreTables.isEmpty()) {
        for (final String table : config.ignoreTables.split(",")) {
          if (!table.isEmpty()) {
            filter = filter.and(new IgnoreTableFilter(table));
          }
        }
      }
      ignoreFilter = filter;
    } catch (final IllegalArgumentException ex) {
      LOG.error("Error creating ignore tables filter: {}", ex.getMessage(), ex);
      issues.add(configIssueFactory.create(
          MySQLBinLogConnectionGroups.ADVANCED.name(), configPrefix + "ignoreTables", Errors.MYSQL_007, ex.getMessage(), ex
      ));
    }
    return ignoreFilter;
  }

  private Filter createIncludeFilter(
      final String configPrefix,
      final MySQLBinLogConfig config,
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    Filter includeFilter = null;
    try {
      // if there are no include filters - pass
      Filter filter = Filters.PASS;
      if (config.includeTables != null && !config.includeTables.isEmpty()) {
        String[] includeTables = config.includeTables.split(",");
        if (includeTables.length > 0) {
          // ignore all that is not explicitly included
          filter = Filters.DISCARD;
          for (final String table : includeTables) {
            if (!table.isEmpty()) {
              filter = filter.or(new IncludeTableFilter(table));
            }
          }
        }
      }
      includeFilter = filter;
    } catch (final IllegalArgumentException ex) {
      LOG.error("Error creating include tables filter: {}", ex.getMessage(), ex);
      issues.add(configIssueFactory.create(
          MySQLBinLogConnectionGroups.ADVANCED.name(), configPrefix + "includeTables", Errors.MYSQL_008, ex.getMessage(), ex
      ));
    }
    return includeFilter;
  }

  private HikariDataSource createDataSource(
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    HikariDataSource result = null;

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(String.format("jdbc:mysql://%s:%d", dataSourceConfig.hostname, dataSourceConfig.port));
    hikariConfig.setUsername(dataSourceConfig.username);
    hikariConfig.setPassword(dataSourceConfig.password);
    hikariConfig.setReadOnly(true);
    hikariConfig.addDataSourceProperty("useSSL", dataSourceConfig.useSSL);
    try {
      result = new HikariDataSource(hikariConfig);
    } catch (final HikariPool.PoolInitializationException e) {
      LOG.error("Error connecting to MySql: {}", e.getMessage(), e);
      issues.add(configIssueFactory.create(
          MySQLBinLogConnectionGroups.MYSQL.name(), null, Errors.MYSQL_003, e.getMessage(), e
      ));
    }

    return result;
  }

  private SourceOffsetFactory createOffsetFactory(
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    SourceOffsetFactory result = null;
    if (dataSource != null) {
      try {
        boolean gtidEnabled = false;
        try {
          gtidEnabled = "ON".equals(Util.getGlobalVariable(dataSource, "gtid_mode"));
        } catch (final SQLException ex) {
          throw Throwables.propagate(ex);
        }
        result = gtidEnabled ? new GtidSourceOffsetFactory() : new BinLogPositionOffsetFactory();
      } catch (final HikariPool.PoolInitializationException ex) {
        LOG.error("Error connecting to MySql: {}", ex.getMessage(), ex);
        issues.add(configIssueFactory.create(
            MySQLBinLogConnectionGroups.MYSQL.name(), null, Errors.MYSQL_003, ex.getMessage(), ex
        ));
      }
    }
    return result;
  }

  private void checkConnection(
      final String connectionPrefix,
      final MySQLBinLogConfig config,
      final ConfigIssueFactory configIssueFactory,
      final List<ConfigIssue> issues
  ) {
    // check if binlog client connection is possible
    // we don't reuse this client later on, it is used just to check that client can connect, it
    // is immediately closed after connection.
    BinaryLogClient client = createBinaryLogClient();
    try {
      client.setKeepAlive(false);
      client.connect(config.connectTimeout);
    } catch (final IOException | TimeoutException ex) {
      LOG.error("Error connecting to MySql binlog: {}", ex.getMessage(), ex);
      issues.add(configIssueFactory.create(
          MySQLBinLogConnectionGroups.MYSQL.name(), connectionPrefix + "connectionString", Errors.MYSQL_003, ex.getMessage(), ex
      ));
    } finally {
      try {
        client.disconnect();
      } catch (final IOException e) {
        LOG.warn("Error disconnecting from MySql: {}", e.getMessage(), e);
      }
    }
  }

  public BinaryLogClient createBinaryLogClient(
  ) {
    BinaryLogClient binLogClient = new BinaryLogClient(
        dataSourceConfig.hostname,
        dataSourceConfig.port,
        dataSourceConfig.username,
        dataSourceConfig.password
    );
    if (dataSourceConfig.useSSL) {
      binLogClient.setSSLMode(SSLMode.REQUIRED);
    } else {
      binLogClient.setSSLMode(SSLMode.DISABLED);
    }
    binLogClient.setServerId(dataSourceConfig.serverId);
    return binLogClient;
  }

  private void loadDrivers() {
    for(final String driverName : MYSQL_DRIVERS) {
      try {
        LOG.info("Loading driver: {}", driverName);
        Class.forName(driverName);
        LOG.info("Loaded driver: {}", driverName);
      } catch (final ClassNotFoundException e) {
        LOG.error("Can't load driver: {}", driverName, e);
      }
    }
  }

  public void destroy() {
    if (dataSource != null) {
      dataSource.close();
    }
  }
}
