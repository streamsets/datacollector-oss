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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.*;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import junit.framework.Assert;
import org.joda.time.Instant;
import org.junit.After;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@SuppressWarnings("Duplicates")
public class TestJdbcTarget {

  private final String username = "sa";
  private final String password = "sa";
  private final String unprivUser = "unpriv_user";
  private final String unprivPassword = "unpriv_pass";
  private final String database = "TEST";
  private final String schema = "TEST";
  private final String tableName = "TEST_TABLE";

  private final String h2ConnectionString = "jdbc:h2:mem:" + database;

  private Connection connection = null;

  @Parameterized.Parameters
  public static Collection<Boolean> caseSensitives() {
    return ImmutableList.of(false, true);
  }

  @Parameterized.Parameter
  public boolean caseSensitive;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
          "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
          "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TABLE_ONE " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TABLE_TWO " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TABLE_THREE " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
        "CREATE TABLE IF NOT EXISTS TEST.DATETIMES (P_ID INT NOT NULL, T TIME, D DATE, DT DATETIME, PRIMARY KEY(P_ID)) "
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS \"TEST\".\"test_table@\"" +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
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
      statement.execute("DROP TABLE IF EXISTS TEST.DATETIMES;");
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
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Record> emptyBatch = ImmutableList.of();
    targetRunner.runInit();
    targetRunner.runWrite(emptyBatch);
    targetRunner.runDestroy();
  }

  @Test
  public void testSingleRecord() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1));
    fields.add(Field.create("Adam"));
    fields.add(Field.create("Kunicki"));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  public void testSingleRecordWithCustomParam() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME", "UPPER(?)"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1));
    fields.add(Field.create("Adam"));
    fields.add(Field.create("Kunicki"));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT FIRST_NAME, LAST_NAME FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals("ADAM", rs.getString(1));
      assertEquals("Kunicki", rs.getString(2));
    }
  }

  @Test
  public void testRecordWithBatchUpdateException() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
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
    fields2.add(Field.create(1));
    fields2.add(Field.create("Jon"));
    fields2.add(Field.create("Natkins"));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create(2));
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.createDatetime(new Instant().toDate()));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(2, rs.getInt(1));
    }

    assertEquals(1, targetRunner.getErrorRecords().size());
  }

  @Test
  public void testRollback() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        true,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
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
    fields2.add(Field.create(1));
    fields2.add(Field.create("Jon"));
    fields2.add(Field.create("Natkins"));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create(2));
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.createDatetime(new Instant().toDate()));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(0, rs.getInt(1));  // the 3rd record should make it
    }
    assertEquals(3, targetRunner.getErrorRecords().size());
  }

  @Test
  public void testRecordWithDataTypeException() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
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
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(2, rs.getInt(1));
    }

    assertEquals(1, targetRunner.getErrorRecords().size());
  }

  @Test
  public void testMultiRowRecordWriterWithDataTypeException() throws Exception {
    thrown.expect(OnRecordErrorException.class);

    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        true,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
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
    targetRunner.runInit();
    targetRunner.runWrite(records);

    // Currently for MultiInsert we will have to hard stop until
    // the MultiRowRecordWriter can be refactored to regenerate the insert statement.
  }

  @Test
  public void testRecordWithBadPermissions() throws Exception {
    thrown.expect(StageException.class);

    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, unprivUser, unprivPassword)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

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
    fields2.add(Field.create("Natkins"));
    fields2.add(Field.createDatetime(new Instant().toDate()));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create(3));
    fields3.add(Field.create("Jon"));
    fields3.add(Field.create("Daulton"));
    fields3.add(Field.createDatetime(new Instant().toDate()));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  public void testBadConnectionString() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean("bad connection string", username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadCredentials() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, "foo", "bar")
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadColumnMapping() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST"),
        new JdbcFieldColumnParamMapping("[2]", "last_name"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(2, issues.size());
  }

  @Test
  public void testMultipleTables() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        "${record:attribute('tableName')}",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Record> records = ImmutableList.of(
        generateRecord(1, "Adam", "Kunicki", "TABLE_ONE"),
        generateRecord(2, "John", "Smith", "TABLE_TWO"),
        generateRecord(3, "Jane", "Doe", "TABLE_TWO"),
        generateRecord(4, "Jane", "Doe", "TABLE_THREE")
    );
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TABLE_ONE");
      rs.next();
      assertEquals(1, rs.getInt(1));
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TABLE_TWO");
      rs.next();
      assertEquals(2, rs.getInt(1));
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TABLE_THREE");
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  public void testEncloseTableNames() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        schema,
        "${record:attribute('tableName')}",
        fieldMappings,
        true, // enclose table name
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Record> records = ImmutableList.of(
        generateRecord(1, "Ji Sun", "Kim", "test_table@")
    );
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.\"test_table@\"");
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }

  private Record generateRecord(int id, String first, String last, String tableName) {
    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(id));
    fields.add(Field.create(first));
    fields.add(Field.create(last));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));
    record.getHeader().setAttribute("tableName", tableName);

    return record;
  }

  @Test
  public void testDateTimeTypes() throws Exception {
    Date d = new Date();

    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "T"),
        new JdbcFieldColumnParamMapping("[2]", "D"),
        new JdbcFieldColumnParamMapping("[3]", "DT")
    );

    Target target = new JdbcTarget(
        schema,
        "DATETIMES",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create(1));
    fields1.add(Field.createTime(d));
    fields1.add(Field.createDate(d));
    fields1.add(Field.createDatetime(d));
    record1.set(Field.create(fields1));

    List<Record> records = ImmutableList.of(record1);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.DATETIMES WHERE P_ID = 1");
      assertTrue(rs.next());
      assertEquals(new SimpleDateFormat("HH:mm:ss").format(d), rs.getTime(2).toString());
      assertEquals(new SimpleDateFormat("YYY-MM-dd").format(d), rs.getDate(3).toString());
      assertEquals(d, rs.getTimestamp(4));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testDateTimeTypesWillNulls() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "T"),
        new JdbcFieldColumnParamMapping("[2]", "D"),
        new JdbcFieldColumnParamMapping("[3]", "DT")
    );

    Target target = new JdbcTarget(
        schema,
        "DATETIMES",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create(2));
    fields1.add(Field.createTime(null));
    fields1.add(Field.createDate(null));
    fields1.add(Field.createDatetime(null));
    record1.set(Field.create(fields1));

    List<Record> records = ImmutableList.of(record1);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.DATETIMES WHERE P_ID = 2");
      assertTrue(rs.next());
      assertEquals(null, rs.getTime(2));
      assertEquals(null, rs.getDate(3));
      assertEquals(null, rs.getTimestamp(4));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testInvalidTableNameFromRecord() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "T"),
        new JdbcFieldColumnParamMapping("[2]", "D"),
        new JdbcFieldColumnParamMapping("[3]", "DT")
    );

    Target target = new JdbcTarget(
        schema,
        "${record:attribute('tableName')}",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        PreparedStatementCache.UNLIMITED_CACHE,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password)
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create(2));
    fields1.add(Field.createTime(null));
    fields1.add(Field.createDate(null));
    fields1.add(Field.createDatetime(null));
    record1.set(Field.create(fields1));
    record1.getHeader().setAttribute("tableName", "FalseTableName");

    List<Record> records = ImmutableList.of(record1);
    targetRunner.runInit();
    try {
      targetRunner.runWrite(records);
    } catch (StageException ex) {
      Assert.fail();
    }

    assertEquals(1, targetRunner.getErrorRecords().size());
    assertEquals(JdbcErrors.JDBC_16.getCode(), targetRunner.getErrorRecords().get(0).getHeader().getErrorCode());
    targetRunner.runDestroy();
  }

}
