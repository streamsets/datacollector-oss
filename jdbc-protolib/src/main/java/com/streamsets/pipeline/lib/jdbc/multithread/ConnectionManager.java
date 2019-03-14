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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Connection Manager to fetch connections as needed.
 */
public final class ConnectionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  private final DatabaseVendor vendor;
  private final HikariDataSource hikariDataSource;
  private final ThreadLocal<Connection> threadLocalConnection;
  private final Set<Connection> connectionsToCloseDuringDestroy;
  private final JdbcUtil jdbcUtil;

  public ConnectionManager(HikariDataSource hikariDataSource) {
    this(DatabaseVendor.UNKNOWN, hikariDataSource);
  }

  public ConnectionManager(DatabaseVendor vendor, HikariDataSource hikariDataSource) {
    this.vendor = vendor;
    this.hikariDataSource = hikariDataSource;
    this.connectionsToCloseDuringDestroy = new HashSet<>();
    this.threadLocalConnection = new ThreadLocal<>();
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
  }

  private synchronized Connection getNewConnection() throws SQLException{
    Connection newConnection = hikariDataSource.getConnection();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("connection transaction level: {}", newConnection.getTransactionIsolation());
      LOGGER.debug("connection auto commit: {}", newConnection.getAutoCommit());
    }
    connectionsToCloseDuringDestroy.add(newConnection);
    return newConnection;
  }

  /**
   * Get {@link Connection} if cached connection is null fetch a connection from {@link HikariDataSource}
   * @return {@link Connection}
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    if (threadLocalConnection.get() == null) {
      threadLocalConnection.set(getNewConnection());
    }
    return threadLocalConnection.get();
  }

  /**
   * Close the current thread's connection
   */
  public void closeConnection() {
    LOGGER.debug("Closing connection");
    Connection connectionToRemove = threadLocalConnection.get();
    jdbcUtil.closeQuietly(connectionToRemove);
    if (connectionToRemove != null) {
      synchronized (this) {
        connectionsToCloseDuringDestroy.remove(connectionToRemove);
      }
    }
    threadLocalConnection.set(null);
  }

  /**
   * Closes all connections
   */
  public synchronized void closeAll() {
    LOGGER.debug("Closing all connections");
    connectionsToCloseDuringDestroy.forEach(jdbcUtil::closeQuietly);
  }

  public DatabaseVendor getVendor() {
    return vendor;
  }
}
