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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.List;
import java.util.SortedMap;

public class TestPreparedStatementMap {

  private final String username = "sa";
  private final String password = "sa";
  private static final String connectionString = "jdbc:h2:mem:test";
  private DataSource dataSource;
  private Connection connection;

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionString);
    config.setUsername(username);
    config.setPassword(password);
    config.setMaximumPoolSize(2);
    dataSource = new HikariDataSource(config);

    connection = dataSource.getConnection();
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
              "(P_ID INT NOT NULL, MSG VARCHAR(255), PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.COMPOSITE_KEY " +
              "(P_ID INT NOT NULL, P_IDB INT NOT NULL, MSG VARCHAR(255), PRIMARY KEY(P_ID, P_IDB));"
      );
      String unprivUser = "unpriv_user";
      String unprivPassword = "unpriv_pass";
      statement.addBatch("CREATE USER IF NOT EXISTS " + unprivUser + " PASSWORD '" + unprivPassword + "';");
      statement.addBatch("GRANT SELECT ON TEST.TEST_TABLE TO " + unprivUser + ";");

      statement.executeBatch();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TABLE;");
    }

    // Last open connection terminates H2
    connection.close();
  }

  @Test
  public void testPreparedStatementMapSinglePrimaryKey() {
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("/field1", "P_ID"),
        new JdbcFieldColumnMapping("/field2", "MSG")
    );
    List<String> primaryKeyColumns = ImmutableList.of("P_ID");
    SortedMap<String, String> columnToParam = ImmutableSortedMap.of(
        "P_ID", "?",
        "MSG", "?"
    );

    boolean caseSensitive = false;
    PreparedStatementMap cacheMap = new PreparedStatementMap(
        connection,
        "TEST.TEST_TABLE",
        generatedColumnMappings,
        primaryKeyColumns,
        10,
        caseSensitive
    );
    try {
      PreparedStatement insert = cacheMap.getPreparedStatement(OperationType.INSERT_CODE, columnToParam);
      Assert.assertTrue(insert.toString().contains("INSERT INTO TEST.TEST_TABLE (MSG, P_ID) VALUES (?, ?)"));

      PreparedStatement delete = cacheMap.getPreparedStatement(OperationType.DELETE_CODE, columnToParam);
      Assert.assertTrue(delete.toString().contains("DELETE FROM TEST.TEST_TABLE WHERE P_ID = ?"));

      PreparedStatement update = cacheMap.getPreparedStatement(OperationType.UPDATE_CODE, columnToParam);
      Assert.assertTrue(update.toString().contains("UPDATE TEST.TEST_TABLE SET MSG = ?, P_ID = ? WHERE P_ID = ?"));
    } catch (StageException ex) {
      Assert.fail("StageException while generating a PreparedStatement:" + ex.getMessage());
    }
  }

  @Test
  public void testPreparedStatementMapSinglePrimaryKeyWithCaseSensitive() {
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("/field1", "P_ID"),
        new JdbcFieldColumnMapping("/field2", "MSG")
    );
    List<String> primaryKeyColumns = ImmutableList.of("P_ID");
    SortedMap<String, String> columnToParam = ImmutableSortedMap.of(
        "P_ID", "?",
        "MSG", "?"
    );

    boolean caseSensitive = true;
    PreparedStatementMap cacheMap = new PreparedStatementMap(
        connection,
        "\"TEST\".\"TEST_TABLE\"",
        generatedColumnMappings,
        primaryKeyColumns,
        10,
        caseSensitive
    );
    try {
      PreparedStatement insert = cacheMap.getPreparedStatement(OperationType.INSERT_CODE, columnToParam);
      Assert.assertTrue(insert.toString().contains("INSERT INTO \"TEST\".\"TEST_TABLE\" (\"MSG\", \"P_ID\") VALUES (?, ?)"));

      PreparedStatement delete = cacheMap.getPreparedStatement(OperationType.DELETE_CODE, columnToParam);
      Assert.assertTrue(delete.toString().contains("DELETE FROM \"TEST\".\"TEST_TABLE\" WHERE \"P_ID\" = ?"));

      PreparedStatement update = cacheMap.getPreparedStatement(OperationType.UPDATE_CODE, columnToParam);
      Assert.assertTrue(update.toString().contains("UPDATE \"TEST\".\"TEST_TABLE\" SET \"MSG\" = ?, \"P_ID\" = ? WHERE \"P_ID\" = ?"));
    } catch (StageException ex) {
      Assert.fail("StageException while generating a PreparedStatement:" + ex.getMessage());
    }
  }

  @Test
  public void testPreparedStatementMapCompoundPrimaryKey() {
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("/field1", "P_ID"),
        new JdbcFieldColumnMapping("/field2", "P_IDB"),
    new JdbcFieldColumnMapping("/field3", "MSG")
    );
    List<String> primaryKeyColumns = ImmutableList.of("P_ID", "P_IDB");
    SortedMap<String, String> columnToParam = ImmutableSortedMap.of(
        "P_ID", "?",
        "P_IDB", "?",
        "MSG", "?"
    );

    boolean caseSensitive = false;
    PreparedStatementMap cacheMap = new PreparedStatementMap(
        connection,
        "TEST.COMPOSITE_KEY",
        generatedColumnMappings,
        primaryKeyColumns,
        -1,
        caseSensitive
    );

    try {
      PreparedStatement insert = cacheMap.getPreparedStatement(OperationType.INSERT_CODE, columnToParam);
      Assert.assertTrue(insert.toString().contains("INSERT INTO TEST.COMPOSITE_KEY (MSG, P_ID, P_IDB) VALUES (?, ?, ?)"));

      PreparedStatement delete = cacheMap.getPreparedStatement(OperationType.DELETE_CODE, columnToParam);
      Assert.assertTrue(delete.toString().contains("DELETE FROM TEST.COMPOSITE_KEY WHERE P_ID = ? AND P_IDB = ?"));

      PreparedStatement update = cacheMap.getPreparedStatement(OperationType.UPDATE_CODE, columnToParam);
      Assert.assertTrue(update.toString().contains(
          "UPDATE TEST.COMPOSITE_KEY SET MSG = ?, P_ID = ?, P_IDB = ? WHERE P_ID = ? AND P_IDB = ?"
      ));
    } catch (StageException ex) {
      Assert.fail("StageException while generating a PreparedStatement:" + ex.getMessage());
    }
  }

  @Test
  public void testPreparedStatementMapCompoundPrimaryKeyWithCaseSensitive() {
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("/field1", "P_ID"),
        new JdbcFieldColumnMapping("/field2", "P_IDB"),
        new JdbcFieldColumnMapping("/field3", "MSG")
    );
    List<String> primaryKeyColumns = ImmutableList.of("P_ID", "P_IDB");
    SortedMap<String, String> columnToParam = ImmutableSortedMap.of(
        "P_ID", "?",
        "P_IDB", "?",
        "MSG", "?"
    );

    boolean caseSensitive = true;
    PreparedStatementMap cacheMap = new PreparedStatementMap(
        connection,
        "\"TEST\".\"COMPOSITE_KEY\"",
        generatedColumnMappings,
        primaryKeyColumns,
        -1,
        caseSensitive
    );

    try {
      PreparedStatement insert = cacheMap.getPreparedStatement(OperationType.INSERT_CODE, columnToParam);
      Assert.assertTrue(insert.toString().contains("INSERT INTO \"TEST\".\"COMPOSITE_KEY\" (\"MSG\", \"P_ID\", \"P_IDB\") VALUES (?, ?, ?)"));

      PreparedStatement delete = cacheMap.getPreparedStatement(OperationType.DELETE_CODE, columnToParam);
      Assert.assertTrue(delete.toString().contains("DELETE FROM \"TEST\".\"COMPOSITE_KEY\" WHERE \"P_ID\" = ? AND \"P_IDB\" = ?"));

      PreparedStatement update = cacheMap.getPreparedStatement(OperationType.UPDATE_CODE, columnToParam);
      Assert.assertTrue(update.toString().contains(
          "UPDATE \"TEST\".\"COMPOSITE_KEY\" SET \"MSG\" = ?, \"P_ID\" = ?, \"P_IDB\" = ? WHERE \"P_ID\" = ? AND \"P_IDB\" = ?"
      ));
    } catch (StageException ex) {
      Assert.fail("StageException while generating a PreparedStatement:" + ex.getMessage());
    }
  }
}
