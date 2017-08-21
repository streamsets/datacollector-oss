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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.SortedMap;

public class TestJDBCBaseRecordWriter {

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
  public void testSinglePrimaryKeys() throws StageException {
    boolean caseSensitive = false;
    JdbcBaseRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        new JdbcRecordReader(),
        caseSensitive
    );

    try {
      writer.lookupPrimaryKeys();
      List<String> primaryKeyColumns = writer.getPrimaryKeyColumns();
      Assert.assertEquals(1, primaryKeyColumns.size());
      Assert.assertEquals("P_ID", primaryKeyColumns.get(0));
    } catch (Exception ex) {
      Assert.fail("lookupPrimaryKeys failed: " + ex.getMessage());
    }
  }

  @Test
  public void testCompoundPrimaryKeys() throws StageException {
    boolean caseSensitive = false;
    JdbcBaseRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "COMPOSITE_KEY",
        false, //rollback
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        new JdbcRecordReader(),
        caseSensitive
    );

    try {
      writer.lookupPrimaryKeys();
      List<String> primaryKeyColumns = writer.getPrimaryKeyColumns();
      Assert.assertEquals(2, primaryKeyColumns.size());
      Assert.assertEquals("P_ID", primaryKeyColumns.get(0));
      Assert.assertEquals("P_IDB", primaryKeyColumns.get(1));
    } catch (Exception ex) {
      Assert.fail("lookupPrimaryKeys failed: " + ex.getMessage());
    }
  }

  @Test
  public void testSetParamsToStatement() throws StageException {
    //Test filling parameters but records has invalid values
    List<JdbcFieldColumnParamMapping> columnMapping = ImmutableList.of(
        new JdbcFieldColumnParamMapping("/field1", "P_ID", "?"),
        new JdbcFieldColumnParamMapping("/field2", "MSG", "?")
    );

    List<JdbcFieldColumnMapping> generatedColumnMapping = ImmutableList.of(
        new JdbcFieldColumnMapping("/field1", "P_ID", "", DataType.INTEGER),
        new JdbcFieldColumnMapping("/field2", "MSG", "", DataType.STRING)
    );

    boolean caseSensitive = false;
    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback
        columnMapping,
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        generatedColumnMapping,
        new JdbcRecordReader(),
        caseSensitive
    );
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("field1", Field.create("")); // this should cause Data conversion error
    fields.put("field2", Field.create(true));
    record.set(Field.create(fields));
    SortedMap<String, String> columnsToParameters = ImmutableSortedMap.of(
        "P_ID", "?",
        "MSG", "?"
    );

    String query = "UPDATE TEST.TEST_TABLE SET  MSG = ? WHERE P_ID = ?";

    try {
      PreparedStatement stmt = connection.prepareStatement(query);
      writer.setParameters(OperationType.UPDATE_CODE, columnsToParameters, record, connection, stmt);
      Assert.fail("Primary key should cause data conversion error while converting empty string to int");
    } catch (OnRecordErrorException ex) {
      // should come here
    } catch (SQLException ex) {
      Assert.fail("Wrong SQLException:" + ex.getMessage());
    }
  }
}
