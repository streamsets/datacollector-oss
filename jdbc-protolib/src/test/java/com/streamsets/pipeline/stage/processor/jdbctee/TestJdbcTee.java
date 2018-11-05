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
package com.streamsets.pipeline.stage.processor.jdbctee;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.*;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@SuppressWarnings("Duplicates")
public class TestJdbcTee {

  private final String username = "sa";
  private final String password = "sa";
  private final String unprivUser = "unpriv_user";
  private final String unprivPassword = "unpriv_pass";
  private final String database = "TEST";
  private final String tableName = "TEST_TABLE";
  private final String h2ConnectionString = "jdbc:h2:mem:" + database;

  private Connection connection = null;

  @Parameterized.Parameters
  public static Collection<Boolean> encloseTableNames() {
    return ImmutableList.of(false, true);
  }

  @Parameterized.Parameter
  public boolean encloseTableName;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch("CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " + "(P_ID IDENTITY, FIRST_NAME " +
          "VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP);");
      statement.addBatch("CREATE TABLE IF NOT EXISTS TEST.TABLE_ONE " + "(P_ID IDENTITY, FIRST_NAME VARCHAR" +
          "(255), LAST_NAME VARCHAR(255), TS TIMESTAMP);");
      statement.addBatch("CREATE TABLE IF NOT EXISTS TEST.TABLE_TWO " + "(P_ID IDENTITY, FIRST_NAME VARCHAR" +
          "(255), LAST_NAME VARCHAR(255), TS TIMESTAMP);");
      statement.addBatch("CREATE TABLE IF NOT EXISTS TEST.TABLE_THREE " + "(P_ID IDENTITY, FIRST_NAME " +
          "VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP);");
      statement.addBatch("CREATE TABLE IF NOT EXISTS \"TEST\".\"test_table@\" " + "(P_ID IDENTITY, FIRST_NAME " +
          "VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP);");
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
      statement.execute("DROP TABLE IF EXISTS TEST.TABLE_ONE;");
      statement.execute("DROP TABLE IF EXISTS TEST.TABLE_TWO;");
      statement.execute("DROP TABLE IF EXISTS TEST.TABLE_THREE;");
      statement.execute("DROP TABLE IF EXISTS \"TEST\".\"test_table@\";");
    }

    // Last open connection terminates H2
    connection.close();
  }

  private HikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    HikariPoolConfigBean bean = new HikariPoolConfigBean();
    bean.connectionString = connectionString;
    bean.useCredentials = true;
    bean.username = () -> username;
    bean.password = () -> password;

    return bean;
  }

  @Test
  public void testEmptyBatch() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", 10)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addOutputLane("lane").build();

    List<Record> emptyBatch = ImmutableList.of();
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(emptyBatch);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordList() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addOutputLane("lane").build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Adam"));
    fields.add(Field.create("Kunicki"));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);
      Assert.assertNotEquals(null, record.get("[3]"));

      int p_id = record.get("[3]").getValueAsInteger();

      connection = DriverManager.getConnection(h2ConnectionString, username, password);
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
        rs.next();
        Assert.assertEquals(p_id, rs.getInt(1));
        Assert.assertEquals("Adam", rs.getString(2));
        Assert.assertEquals("Kunicki", rs.getString(3));
        Assert.assertEquals(false, rs.next());
      }
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordMap() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "/first_name",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("/last_name", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("/timestamp", "TS")
    );
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping(
        "P_ID",
        "/p_id"
    ));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addOutputLane("lane").build();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("first_name", Field.create("Adam"));
    fields.put("last_name", Field.create("Kunicki"));
    fields.put("timestamp", Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);
      Assert.assertNotEquals(null, record.get("/p_id"));

      int p_id = record.get("/p_id").getValueAsInteger();

      connection = DriverManager.getConnection(h2ConnectionString, username, password);
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
        rs.next();
        Assert.assertEquals(p_id, rs.getInt(1));
        Assert.assertEquals("Adam", rs.getString(2));
        Assert.assertEquals("Kunicki", rs.getString(3));
        Assert.assertEquals(false, rs.next());
      }
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordWithCustomParam() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME",
            "UPPER(?)"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane").build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Adam"));
    fields.add(Field.create("Kunicki"));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);
      Assert.assertNotEquals(null, record.get("[3]"));

      int p_id = record.get("[3]").getValueAsInteger();

      connection = DriverManager.getConnection(h2ConnectionString, username, password);
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
        rs.next();
        Assert.assertEquals(p_id, rs.getInt(1));
        Assert.assertEquals("ADAM", rs.getString(2));
        Assert.assertEquals("Kunicki", rs.getString(3));
        Assert.assertEquals(false, rs.next());
      }
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testMultiRecordMultiInsert() throws Exception {
    try {
      List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping("[0]",
          "FIRST_NAME"
      ), new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"), new JdbcFieldColumnParamMapping("[2]", "TS"));
      List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

      JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
      processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

      ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
          .addConfiguration("schema", database)
          .addConfiguration("tableNameTemplate", tableName)
          .addConfiguration("customMappings", fieldMappings)
          .addConfiguration("encloseTableName", encloseTableName)
          .addConfiguration("rollbackOnError", false)
          .addConfiguration("useMultiRowOp", true)
          .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
          .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
          .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
          .addConfiguration("generatedColumnMappings", generatedColumnMappings)
          .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
          .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
          .addOutputLane("lane")
          .setOnRecordError(OnRecordError.TO_ERROR)
          .build();

      Record record1 = RecordCreator.create();
      List<Field> fields1 = new ArrayList<>();
      fields1.add(Field.create("Adam"));
      fields1.add(Field.create("Kunicki"));
      fields1.add(Field.createDatetime(new Instant().toDate()));
      record1.set(Field.create(fields1));

      Record record2 = RecordCreator.create();
      List<Field> fields2 = new ArrayList<>();
      fields2.add(Field.create("Jon"));
      fields2.add(Field.create("Natkins"));
      fields2.add(Field.createDatetime(new Instant().toDate()));
      record2.set(Field.create(fields2));

      Record record3 = RecordCreator.create();
      List<Field> fields3 = new ArrayList<>();
      fields3.add(Field.create("Jon"));
      fields3.add(Field.create("Daulton"));
      fields3.add(Field.createDatetime(new Instant().toDate()));
      record3.set(Field.create(fields3));

      List<Record> records = ImmutableList.of(record1, record2, record3);
      processorRunner.runInit();
      processorRunner.runProcess(records);

      connection = DriverManager.getConnection(h2ConnectionString, username, password);
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
        rs.next();
        assertEquals(3, rs.getInt(1));
      }

      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("Adam", rs.getString(2));
        Assert.assertEquals("Kunicki", rs.getString(3));
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("Jon", rs.getString(2));
        Assert.assertEquals("Natkins", rs.getString(3));
        rs.next();
        Assert.assertEquals(3, rs.getInt(1));
        Assert.assertEquals("Jon", rs.getString(2));
        Assert.assertEquals("Daulton", rs.getString(3));
        Assert.assertEquals(false, rs.next());
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test
  public void testMultiRecordSingleInsert() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );
    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane")
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create("Adam"));
    fields1.add(Field.create("Kunicki"));
    fields1.add(Field.createDatetime(new Instant().toDate()));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create("Jon"));
    fields2.add(Field.create("Natkins"));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.createDatetime(new Instant().toDate()));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    processorRunner.runProcess(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(3, rs.getInt(1));
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
      Assert.assertEquals("Adam", rs.getString(2));
      Assert.assertEquals("Kunicki", rs.getString(3));
      rs.next();
      Assert.assertEquals(2, rs.getInt(1));
      Assert.assertEquals("Jon", rs.getString(2));
      Assert.assertEquals("Natkins", rs.getString(3));
      rs.next();
      Assert.assertEquals(3, rs.getInt(1));
      Assert.assertEquals("Jon", rs.getString(2));
      Assert.assertEquals("Daulton", rs.getString(3));
      Assert.assertEquals(false, rs.next());
    }
  }

  @Test
  public void testRecordWithDataTypeException() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[1]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create(1));
    fields1.add(Field.create("Adam"));
    fields1.add(Field.create("Kunicki"));
    fields1.add(Field.createDatetime(new Instant().toDate()));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create(2));
    fields2.add(Field.create("Jon"));
    // Nulls will be interpreted as the a null of the target schema type regardless of Field.Type.
    fields2.add(Field.create(Field.Type.INTEGER, null));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create(3));
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.create("2015011705:30:00"));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    processorRunner.runProcess(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(2, rs.getInt(1));
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
      Assert.assertEquals("Adam", rs.getString(2));
      Assert.assertEquals("Kunicki", rs.getString(3));
      rs.next();
      Assert.assertEquals(2, rs.getInt(1));
      Assert.assertEquals("Jon", rs.getString(2));
      Assert.assertEquals(null, rs.getString(3));
      Assert.assertEquals(false, rs.next());
    }

    assertEquals(1, processorRunner.getErrorRecords().size());
  }

  @Test
  public void testMultiRowRecordWriterWithDataTypeException() throws Exception {
    thrown.expect(OnRecordErrorException.class);

    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", true)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create("Adam"));
    fields1.add(Field.create("Kunicki"));
    fields1.add(Field.createDatetime(new Instant().toDate()));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create("Jon"));
    // Nulls will be interpreted as the a null of the target schema type regardless of Field.Type.
    fields2.add(Field.create(Field.Type.INTEGER, null));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.create("2015011705:30:00"));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    processorRunner.runProcess(records);

    // Currently for MultiInsert we will have to hard stop until
    // the MultiRowRecordWriter can be refactored to regenerate the insert statement.
  }

  @Test
  public void testRecordWithBadPermissions() throws Exception {
    thrown.expect(StageException.class);

    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, unprivUser, unprivPassword);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane")
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create("Adam"));
    fields1.add(Field.create("Kunicki"));
    fields1.add(Field.createDatetime(new Instant().toDate()));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create("Jon"));
    fields2.add(Field.create("Natkins"));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.createDatetime(new Instant().toDate()));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    processorRunner.runProcess(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  public void testBadConnectionString() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean("bad connection string", username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadCredentials() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, "foo", "bar");

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadColumnMapping() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping("[0]", "FIRST"),
        new JdbcFieldColumnParamMapping("[1]", "last_name"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", tableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane").setOnRecordError(
        OnRecordError.TO_ERROR)
        .build();

    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(2, issues.size());
  }

  @Test
  public void testMultipleTables() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", "${record:attribute('tableName')}")
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", encloseTableName)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane").setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> records = ImmutableList.of(
        generateRecord("Adam", "Kunicki", "TABLE_ONE"),
        generateRecord("John", "Smith", "TABLE_TWO"),
        generateRecord("Jane", "Doe", "TABLE_TWO"),
        generateRecord("Jane", "Doe", "TABLE_THREE")
    );
    processorRunner.runInit();
    processorRunner.runProcess(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TABLE_ONE");
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
      Assert.assertEquals("Adam", rs.getString(2));
      Assert.assertEquals("Kunicki", rs.getString(3));
      Assert.assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TABLE_TWO");
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
      Assert.assertEquals("John", rs.getString(2));
      Assert.assertEquals("Smith", rs.getString(3));
      rs.next();
      Assert.assertEquals(2, rs.getInt(1));
      Assert.assertEquals("Jane", rs.getString(2));
      Assert.assertEquals("Doe", rs.getString(3));
      Assert.assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM TEST.TABLE_THREE");
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
      Assert.assertEquals("Jane", rs.getString(2));
      Assert.assertEquals("Doe", rs.getString(3));
      Assert.assertEquals(false, rs.next());
    }
  }

  @Test
  public void testEncloseTableNames() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(new JdbcFieldColumnParamMapping(
            "[0]",
            "FIRST_NAME"
        ),
        new JdbcFieldColumnParamMapping("[1]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "TS")
    );

    List<JdbcFieldColumnMapping> generatedColumnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcTeeDProcessor processor = new JdbcTeeDProcessor();
    processor.hikariConfigBean = createConfigBean(h2ConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcTeeDProcessor.class, processor)
        .addConfiguration("schema", database)
        .addConfiguration("tableNameTemplate", "${record:attribute('tableName')}")
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("encloseTableName", true)
        .addConfiguration("customMappings", fieldMappings)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("useMultiRowOp", false)
        .addConfiguration("maxPrepStmtParameters", JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS)
        .addConfiguration("maxPrepStmtCache", PreparedStatementCache.UNLIMITED_CACHE)
        .addConfiguration("changeLogFormat", ChangeLogFormat.NONE)
        .addConfiguration("unsupportedAction", UnsupportedOperationAction.DISCARD)
        .addConfiguration("defaultOperation", JDBCOperationType.INSERT)
        .addConfiguration("generatedColumnMappings", generatedColumnMappings)
        .addOutputLane("lane").setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> records = ImmutableList.of(
        generateRecord("Ji Sun", "Kim", "test_table@")
    );
    processorRunner.runInit();
    processorRunner.runProcess(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT P_ID, FIRST_NAME, LAST_NAME FROM \"TEST\".\"test_table@\"");
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
      Assert.assertEquals("Ji Sun", rs.getString(2));
      Assert.assertEquals("Kim", rs.getString(3));
      Assert.assertEquals(false, rs.next());
    }
  }

  private Record generateRecord(String first, String last, String tableName) {
    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(first));
    fields.add(Field.create(last));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));
    record.getHeader().setAttribute("tableName", tableName);

    return record;
  }
}
