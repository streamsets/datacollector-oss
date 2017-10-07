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
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class TestGenericRecordWriter {

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
  public void testInsert() throws Exception {
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("P_ID", Field.create(100));
    fields.put("MSG", Field.create("unit test"));
    record.set(Field.create(fields));
    // record doesn't have operation code in header, but default is set to INSERT.
    boolean caseSensitive = false;

    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback set to false
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.USE_DEFAULT,
        new JdbcRecordReader(),
        caseSensitive
    );
    List<Record> batch = ImmutableList.of(record);
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  public void testUpdate() throws Exception {
    Record insertRecord = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(200));
    insert.put("MSG", Field.create("first message"));
    insertRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.INSERT_CODE)
    );
    insertRecord.set(Field.create(insert));

    Record updateRecord = RecordCreator.create();
    Map<String, Field> update = new HashMap<>();
    update.put("P_ID", Field.create(200));
    update.put("MSG", Field.create("second message"));
    updateRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.UPDATE_CODE)
    );
    updateRecord.set(Field.create(update));

    boolean caseSensitive = false;

    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.USE_DEFAULT,
        new JdbcRecordReader(),
        caseSensitive
    );
    List<Record> batch = ImmutableList.of(insertRecord, updateRecord);
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.TEST_TABLE WHERE P_ID = 200");
      rs.next();
      assertEquals(200, rs.getInt(1));
      assertEquals("second message", rs.getString(2));
    }
  }

  @Test
  public void testDelete() throws Exception {
    Record insertRecord = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(300));
    insert.put("MSG", Field.create("message"));
    insertRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.INSERT_CODE)
    );
    insertRecord.set(Field.create(insert));

    Record deleteRecord = RecordCreator.create();
    Map<String, Field> delete = new HashMap<>();
    delete.put("P_ID", Field.create(300));
    delete.put("MSG", Field.create("message"));
    deleteRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.DELETE_CODE)
    );
    deleteRecord.set(Field.create(delete));

    boolean caseSensitive = false;

    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback false
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.USE_DEFAULT,
        new JdbcRecordReader(),
        caseSensitive
    );
    List<Record> batch = ImmutableList.of(insertRecord, deleteRecord);
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  public void testUpdateWithCompositeKey() throws Exception {
    Record insertRecord = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(200));
    insert.put("P_IDB", Field.create(250));
    insert.put("MSG", Field.create("first message"));
    insertRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.INSERT_CODE)
    );
    insertRecord.set(Field.create(insert));

    Record updateRecord = RecordCreator.create();
    Map<String, Field> update = new HashMap<>();
    update.put("P_ID", Field.create(200));
    update.put("P_IDB", Field.create(250));
    update.put("MSG", Field.create("second message"));
    updateRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.UPDATE_CODE)
    );
    updateRecord.set(Field.create(update));

    boolean caseSensitive = false;

    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "COMPOSITE_KEY",
        false,
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.USE_DEFAULT,
        new JdbcRecordReader(),
        caseSensitive
    );
    List<Record> batch = ImmutableList.of(insertRecord, updateRecord);
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.COMPOSITE_KEY WHERE P_ID = 200 AND P_IDB = 250");
      rs.next();
      assertEquals(200, rs.getInt(1));
      assertEquals(250, rs.getInt(2));
      assertEquals("second message", rs.getString(3));
    }
  }

  @Test
  public void testDeleteWithCompositeKey() throws Exception {
    Record insertRecord = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(300));
    insert.put("P_IDB", Field.create(350));
    insert.put("MSG", Field.create("message"));
    insertRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.INSERT_CODE)
    );
    insertRecord.set(Field.create(insert));

    Record deleteRecord = RecordCreator.create();
    Map<String, Field> delete = new HashMap<>();
    delete.put("P_ID", Field.create(300));
    delete.put("P_IDB", Field.create(350));
    delete.put("MSG", Field.create("message"));
    deleteRecord.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.DELETE_CODE)
    );
    deleteRecord.set(Field.create(delete));

    boolean caseSensitive = false;

    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "COMPOSITE_KEY",
        false,
        new LinkedList<JdbcFieldColumnParamMapping>(),
        PreparedStatementCache.UNLIMITED_CACHE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.USE_DEFAULT,
        new JdbcRecordReader(),
        caseSensitive
    );
    List<Record> batch = ImmutableList.of(insertRecord, deleteRecord);
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.COMPOSITE_KEY");
      rs.next();
      assertEquals(0, rs.getInt(1));
    }
  }

  /**
   * Testing setParameters in JdbcGenericRecordWriter as well as setParamsToStatement()
   * and setPrimaryKeys() in JdbcBaseRecordWriter.
   * @throws StageException
   */
  @Test
  public void testSetParameters() throws StageException {
    List<JdbcFieldColumnParamMapping> columnMapping = ImmutableList.of(
        new JdbcFieldColumnParamMapping("/field1", "P_ID", "?"),
        new JdbcFieldColumnParamMapping("/field2", "MSG", "?")
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
        new JdbcRecordReader(),
        caseSensitive
    );
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("field1", Field.create(100));
    fields.put("field2", Field.create("StreamSets"));
    record.set(Field.create(fields));

    SortedMap<String, String> columnsToParameters = new TreeMap<>();
    columnsToParameters.put("P_ID", "?");
    columnsToParameters.put("MSG", "?");

    String query = "INSERT INTO TEST.TEST_TABLE (MSG, P_ID) VALUES (?, ?)";
    executeSetParameters(OperationType.INSERT_CODE, query, writer, columnsToParameters, record);

    query = "DELETE FROM TEST.TEST_TABLE  WHERE P_ID = ?";
    executeSetParameters(OperationType.DELETE_CODE, query, writer, columnsToParameters, record);

    query = "UPDATE TEST.TEST_TABLE SET  MSG = ? WHERE P_ID = ?";
    fields.put("field2", Field.create("This is an updated message"));
    columnsToParameters.remove("P_ID");
    executeSetParameters(OperationType.UPDATE_CODE, query, writer, columnsToParameters, record);
  }

  private void executeSetParameters(
      int op,
      String query,
      JdbcGenericRecordWriter writer,
      SortedMap<String, String> columnsToParameters,
      Record record
  ) throws OnRecordErrorException {
    try {
      PreparedStatement stmt = connection.prepareStatement(query);
      writer.setParameters(op, columnsToParameters, record, connection, stmt);
      stmt.execute(); // should succeed
    } catch (SQLException ex) {
      Assert.fail("Query failed with " + ex.getMessage());
    }
  }
}
