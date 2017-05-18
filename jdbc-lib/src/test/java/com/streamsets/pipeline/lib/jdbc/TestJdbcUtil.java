/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJdbcUtil {

  private final String username = "sa";
  private final String password = "sa";
  private final String database = "test";
  private final String h2ConnectionString = "jdbc:h2:mem:" + database;
  private final String schema = "SCHEMA_TEST";
  private final String tableName = "MYAPP";
  private final String tableNameWithSpecialChars = "MYAPP.TEST_TABLE1.CUSTOMER";

  private HikariPoolConfigBean createConfigBean() {
    HikariPoolConfigBean bean = new HikariPoolConfigBean();
    bean.connectionString = h2ConnectionString;
    bean.username = username;
    bean.password = password;

    return bean;
  }

  private Connection connection;

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(h2ConnectionString);
    config.setUsername(username);
    config.setPassword(password);
    config.setMaximumPoolSize(2);
    HikariDataSource dataSource = new HikariDataSource(config);

    connection = dataSource.getConnection();
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS " + schema + ";");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schema + "." + tableName +
              "(P_ID INT NOT NULL, MSG VARCHAR(255), PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schema + "." + "\"" + tableNameWithSpecialChars + "\"" +
              "(P_ID INT NOT NULL, P_IDB INT NOT NULL, MSG VARCHAR(255), PRIMARY KEY(P_ID, P_IDB));"
      );
      String unprivUser = "unpriv_user";
      String unprivPassword = "unpriv_pass";
      statement.addBatch("CREATE USER IF NOT EXISTS " + unprivUser + " PASSWORD '" + unprivPassword + "';");
      //statement.addBatch("GRANT SELECT ON TEST.TEST_TABLE TO " + unprivUser + ";");

      statement.executeBatch();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP TABLE IF EXISTS " + schema + ".\"MYAPP.TEST_TABLE1.CUSTOMER\";");
      statement.execute("DROP TABLE IF EXISTS " + schema + ".MYAPP;");
    }
    // Last open connection terminates H2
    connection.close();
  }

  @Test
  public void testTransactionIsolation() throws Exception {
    HikariPoolConfigBean config = createConfigBean();
    config.transactionIsolation = TransactionIsolationLevel.TRANSACTION_READ_COMMITTED;

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config, new Properties());
    Connection connection = dataSource.getConnection();
    assertNotNull(connection);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
  }


  @Test
  public void testGetTableMetadata() throws Exception {
    HikariPoolConfigBean config = createConfigBean();

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config, new Properties());
    Connection connection = dataSource.getConnection();

    boolean caseSensitive = false;
    ResultSet resultSet = JdbcUtil.getTableMetadata(connection, schema, tableName, caseSensitive);
    assertEquals(true, resultSet.next());
  }

  @Test
  public void testGetTableMetadataWithDots() throws Exception {
    HikariPoolConfigBean config = createConfigBean();

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config, new Properties());
    Connection connection = dataSource.getConnection();

    boolean caseSensitive = true;

    ResultSet resultSet = JdbcUtil.getTableMetadata(connection, schema, tableNameWithSpecialChars, caseSensitive);
    assertEquals(true, resultSet.next());
  }



}
