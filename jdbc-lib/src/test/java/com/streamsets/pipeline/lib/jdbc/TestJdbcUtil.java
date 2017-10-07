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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.junit.Assert.assertThat;

public class TestJdbcUtil {

  private static final long MINUS_2HRS_OFFSET = -7200000L;
  private final String username = "sa";
  private final String password = "sa";
  private final String database = "test";
  private final String h2ConnectionString = "jdbc:h2:mem:" + database;
  private final String schema = "SCHEMA_TEST";
  private final String tableName = "MYAPP";
  private final String tableNameWithSpecialChars = "MYAPP.TEST_TABLE1.CUSTOMER";
  private final String emptyTableName = "EMPTY_TABLE";
  private final String dataTypesTestTable = "DATA_TYPES_TEST";

  private HikariPoolConfigBean createConfigBean() {
    HikariPoolConfigBean bean = new HikariPoolConfigBean();
    bean.connectionString = h2ConnectionString;
    bean.useCredentials = true;
    bean.username = () -> username;
    bean.password = () -> password;

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
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schema + "." + dataTypesTestTable +
              "(P_ID INT NOT NULL, TS_WITH_TZ TIMESTAMP WITH TIME ZONE NOT NULL);"
      );
      statement.addBatch(
          "INSERT INTO " + schema + "." + dataTypesTestTable + " VALUES (1, CAST('1970-01-01 00:00:00+02:00' " +
              "AS TIMESTAMP WITH TIME ZONE));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schema + "." + "\"" + emptyTableName + "\"" +
              "(P_ID TIMESTAMP NOT NULL, PRIMARY KEY(P_ID));"
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
      statement.execute("DROP TABLE IF EXISTS " + schema + "." + dataTypesTestTable);
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

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config);
    Connection connection = dataSource.getConnection();
    assertNotNull(connection);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
  }


  @Test
  public void testGetTableMetadata() throws Exception {
    HikariPoolConfigBean config = createConfigBean();

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config);
    Connection connection = dataSource.getConnection();

    boolean caseSensitive = false;
    ResultSet resultSet = JdbcUtil.getTableMetadata(connection, schema, tableName, caseSensitive);
    assertEquals(true, resultSet.next());
  }

  @Test
  public void testGetTableMetadataWithDots() throws Exception {
    HikariPoolConfigBean config = createConfigBean();

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config);
    Connection connection = dataSource.getConnection();

    boolean caseSensitive = true;

    ResultSet resultSet = JdbcUtil.getTableMetadata(connection, schema, tableNameWithSpecialChars, caseSensitive);
    assertEquals(true, resultSet.next());
  }

  @Test
  public void testResultToField() throws Exception {
    HikariPoolConfigBean config = createConfigBean();
    try (HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config)) {
      try (Connection connection = dataSource.getConnection()) {
        try (Statement stmt = connection.createStatement()) {
          // Currently only validates TIMESTAMP WITH TIME ZONE (H2 does not support TIME WITH TIME ZONE)
          ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + schema + "." + dataTypesTestTable);
          assertTrue(resultSet.next());
          Field field = JdbcUtil.resultToField(
            resultSet.getMetaData(),
            resultSet,
            2,
            0,
            0,
            UnknownTypeAction.STOP_PIPELINE
          );
          assertEquals(Field.Type.ZONED_DATETIME, field.getType());
          assertEquals(
              ZonedDateTime.ofInstant(Instant.ofEpochMilli(MINUS_2HRS_OFFSET), ZoneId.ofOffset(
                  "",
                  ZoneOffset.ofHours(2)
              )),
              field.getValueAsZonedDateTime()
          );
        }
      }
    }
  }

  @Test
  public void testGetMinValues() throws Exception {
    HikariPoolConfigBean config = createConfigBean();

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config);
    Connection connection = dataSource.getConnection();

    Map<String, String> emptyTableMin = JdbcUtil.getMinimumOffsetValues(
        connection,
        schema,
        emptyTableName,
        QuoteChar.NONE,
        Arrays.asList("P_ID")
    );
    assertThat(emptyTableMin.size(), equalTo(0));

    Map<String, String> typedTableMin = JdbcUtil.getMinimumOffsetValues(
        connection,
        schema,
        dataTypesTestTable,
        QuoteChar.NONE,
        Arrays.asList("P_ID")
    );
    assertThat(typedTableMin.size(), equalTo(1));
    assertThat(typedTableMin, hasEntry("P_ID", "1"));
  }

}
