/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Connection Manager to fetch connections as needed.
 */
public final class ConnectionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  private HikariDataSource hikariDataSource;
  private Connection connection = null;

  public ConnectionManager(HikariDataSource hikariDataSource) {
    this.hikariDataSource = hikariDataSource;
  }

  /**
   * Get {@link Connection} if the cached connection is null fetch a connection from {@link HikariDataSource}
   * @return {@link Connection}
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    if (connection == null) {
      connection = hikariDataSource.getConnection();
    }
    return connection;
  }

  /**
   * Close the cached connection
   */
  public void closeConnection() {
    LOGGER.debug("Closing connection");
    JdbcUtil.closeQuietly(connection);
    connection = null;
  }
}
