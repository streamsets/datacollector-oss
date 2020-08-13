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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.*;
import com.streamsets.pipeline.lib.jdbc.connection.JdbcConnection;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import junit.framework.Assert;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@SuppressWarnings("Duplicates")
@Ignore //SCJDBC-132. Migrate this class to STF
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
    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    createSchemaAndTables("TEST");
    createSchemaAndTables("TEST_EXTRA");
  }

  @After
  public void tearDown() throws SQLException {
    dropSchemaAndTables("TEST");
    dropSchemaAndTables("TEST_EXTRA");

    // Last open connection terminates H2
    connection.close();
  }

  private void createSchemaAndTables(String schemaName) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS " + schemaName + ";");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".TEST_TABLE " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".TABLE_ONE " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".TABLE_TWO " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".TABLE_THREE " +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".DATETIMES (P_ID INT NOT NULL, T TIME, D DATE, " +
              "DT DATETIME, PRIMARY KEY(P_ID)) "
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".NOT_NULLS (P_ID INT NOT NULL, " +
              "NAME VARCHAR(255) NOT NULL, SURNAME VARCHAR(255) NOT NULL, PRIMARY KEY(P_ID)) "
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS \"" + schemaName + "\".\"test_table@\"" +
              "(P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), " +
              "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS " + schemaName + ".ARRAY_TABLE (P_ID INT NOT NULL, A1 ARRAY, A2 ARRAY, " +
              "PRIMARY KEY(P_ID)) "
      );
      statement.addBatch("CREATE USER IF NOT EXISTS " + unprivUser + " PASSWORD '" + unprivPassword + "';");
      statement.addBatch("GRANT SELECT ON TEST.TEST_TABLE TO " + unprivUser + ";");

      statement.executeBatch();
    }
  }

  private void dropSchemaAndTables(String schemaName) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + ".TEST_TABLE;");
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + ".TABLE_ONE;");
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + ".TABLE_TWO;");
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + ".TABLE_THREE;");
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + ".DATETIMES;");
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + "NOT_NULLS;");
      statement.addBatch("DROP TABLE IF EXISTS " + schemaName + "ARRAY_TABLE;");
      statement.addBatch("DROP TABLE IF EXISTS \"" + schemaName + "\".\"test_table@\";");
      statement.addBatch("DROP SCHEMA " + schemaName + ";");
      statement.executeBatch();
    }
  }

  private JdbcHikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    JdbcHikariPoolConfigBean bean = new JdbcHikariPoolConfigBean();
    bean.connection = new JdbcConnection();
    bean.connection.connectionString = connectionString;
    bean.connection.useCredentials = true;
    bean.connection.username = () -> username;
    bean.connection.password = () -> password;

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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, unprivUser, unprivPassword),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean("bad connection string", username, password),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, "foo", "bar"),
        Collections.emptyList()
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(2, issues.size());
  }

  @Test
  public void testMultipleSchemasAndTables() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        "${record:attribute('schemaName')}",
        "${record:attribute('tableName')}",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Record> records = ImmutableList.of(
        generateRecord(1, "Adam", "Kunicki", "TEST", "TABLE_ONE"),
        generateRecord(3, "John", "Smith", "TEST", "TABLE_THREE"),
        generateRecord(2, "Jane", "Doe", "TEST_EXTRA", "TABLE_ONE"),
        generateRecord(4, "John", "Snow", "TEST_EXTRA", "TABLE_TWO")
    );

    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.TABLE_ONE");
      rs.next();
      assertEquals(1, rs.getInt("P_ID"));
      assertEquals("Adam", rs.getString("FIRST_NAME"));
      assertEquals("Kunicki", rs.getString("LAST_NAME"));
      assertEquals(rs.next(), false);
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.TABLE_THREE");
      rs.next();
      assertEquals(3, rs.getInt("P_ID"));
      assertEquals("John", rs.getString("FIRST_NAME"));
      assertEquals("Smith", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST_EXTRA.TABLE_ONE");
      rs.next();
      assertEquals(2, rs.getInt("P_ID"));
      assertEquals("Jane", rs.getString("FIRST_NAME"));
      assertEquals("Doe", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST_EXTRA.TABLE_TWO");
      rs.next();
      assertEquals(4, rs.getInt("P_ID"));
      assertEquals("John", rs.getString("FIRST_NAME"));
      assertEquals("Snow", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }
  }

  @Test
  public void testMultipleSchemas() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
        new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"),
        new JdbcFieldColumnParamMapping("[3]", "TS")
    );

    Target target = new JdbcTarget(
        "${record:attribute('schemaName')}",
        tableName,
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    List<Record> records = ImmutableList.of(
        generateRecord(1, "Adam", "Kunicki", "TEST", tableName),
        generateRecord(3, "John", "Smith", "TEST", tableName),
        generateRecord(2, "Jane", "Doe", "TEST_EXTRA", tableName),
        generateRecord(4, "John", "Snow", "TEST_EXTRA", tableName)
    );

    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s.%s", "TEST", tableName));
      rs.next();
      assertEquals(1, rs.getInt("P_ID"));
      assertEquals("Adam", rs.getString("FIRST_NAME"));
      assertEquals("Kunicki", rs.getString("LAST_NAME"));
      rs.next();
      assertEquals(3, rs.getInt("P_ID"));
      assertEquals("John", rs.getString("FIRST_NAME"));
      assertEquals("Smith", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s.%s", "TEST_EXTRA", tableName));
      rs.next();
      assertEquals(2, rs.getInt("P_ID"));
      assertEquals("Jane", rs.getString("FIRST_NAME"));
      assertEquals("Doe", rs.getString("LAST_NAME"));
      rs.next();
      assertEquals(4, rs.getInt("P_ID"));
      assertEquals("John", rs.getString("FIRST_NAME"));
      assertEquals("Snow", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.TABLE_ONE");
      rs.next();
      assertEquals(1, rs.getInt("P_ID"));
      assertEquals("Adam", rs.getString("FIRST_NAME"));
      assertEquals("Kunicki", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.TABLE_TWO");
      rs.next();
      assertEquals(2, rs.getInt("P_ID"));
      assertEquals("John", rs.getString("FIRST_NAME"));
      assertEquals("Smith", rs.getString("LAST_NAME"));
      rs.next();
      assertEquals(3, rs.getInt("P_ID"));
      assertEquals("Jane", rs.getString("FIRST_NAME"));
      assertEquals("Doe", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
    }

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.TABLE_THREE");
      rs.next();
      assertEquals(4, rs.getInt("P_ID"));
      assertEquals("Jane", rs.getString("FIRST_NAME"));
      assertEquals("Doe", rs.getString("LAST_NAME"));
      assertEquals(false, rs.next());
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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

  private Record generateRecord(int id, String first, String last, String schemaName, String tableName) {
    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(id));
    fields.add(Field.create(first));
    fields.add(Field.create(last));
    fields.add(Field.createDatetime(new Instant().toDate()));
    record.set(Field.create(fields));
    record.getHeader().setAttribute("schemaName", schemaName);
    record.getHeader().setAttribute("tableName", tableName);

    return record;
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
      assertEquals(new SimpleDateFormat("yyyy-MM-dd").format(d), rs.getDate(3).toString());
      assertEquals(d, rs.getTimestamp(4));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testInvalidDateTimeValue() throws Exception {
    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "DT")
    );

    Target target = new JdbcTarget(
        schema,
        "DATETIMES",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create(100));
    fields1.add(Field.create("This isn't really a timestmap, right?"));
    record1.set(Field.create(fields1));

    List<Record> records = ImmutableList.of(record1);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    Assert.assertEquals(JdbcErrors.JDBC_23.name(), targetRunner.getErrorRecords().get(0).getHeader().getErrorCode());
  }

  @Test
  public void testArrays() throws Exception {
    String[] strings = {"abc", "def", "ghi"};
    int[] ints = {1, 2, 3};

    List<JdbcFieldColumnParamMapping> fieldMappings = ImmutableList.of(
        new JdbcFieldColumnParamMapping("[0]", "P_ID"),
        new JdbcFieldColumnParamMapping("[1]", "A1"),
        new JdbcFieldColumnParamMapping("[2]", "A2")
    );

    Target target = new JdbcTarget(
        schema,
        "ARRAY_TABLE",
        fieldMappings,
        caseSensitive,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create(1));

    List<Field> stringFieldList = new ArrayList<>();
    for (int i = 0; i < strings.length; i++) {
      stringFieldList.add(Field.create(strings[i]));
    }
    fields1.add(Field.create(stringFieldList));

    List<Field> intFieldList = new ArrayList<>();
    for (int i = 0; i < ints.length; i++) {
      intFieldList.add(Field.create(ints[i]));
    }
    fields1.add(Field.create(intFieldList));

    record1.set(Field.create(fields1));

    List<Record> records = ImmutableList.of(record1);
    targetRunner.runInit();
    targetRunner.runWrite(records);

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM TEST.ARRAY_TABLE WHERE P_ID = 1");
      assertTrue(rs.next());

      Object[] stringArray = (Object[])rs.getArray(2).getArray();
      assertEquals(strings.length, stringArray.length);
      for (int i = 0; i < strings.length; i++) {
        assertEquals(strings[i], stringArray[i]);
      }

      Object[] intArray = (Object[])rs.getArray(3).getArray();
      assertEquals(ints.length, intArray.length);
      for (int i = 0; i < ints.length; i++) {
        assertEquals(ints[i], intArray[i]);
      }

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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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

  @Test  // SDC-9267
  public void testInsertNullValuesWhenNotAllowed() throws Exception {
    Target target = new JdbcTarget(
        schema,
      "NOT_NULLS",
        Collections.emptyList(),
        false,
        false,
        false,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS,
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
    );
    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();

    Record noNameSurnameRecord = RecordCreator.create("a", "insert");
    noNameSurnameRecord.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of("P_ID", Field.create(2))));

    Record noSurnameRecord = RecordCreator.create("a", "update");
    noSurnameRecord.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of("P_ID", Field.create(2), "NAME", Field.create("Secret"))));

    Record correctRecord = RecordCreator.create("a", "update");
    correctRecord.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of("P_ID", Field.create(2), "NAME", Field.create("Secret"), "SURNAME", Field.create("Mr. Awesome"))));

    targetRunner.runInit();
    targetRunner.runWrite(ImmutableList.of(noNameSurnameRecord, noSurnameRecord, correctRecord));

    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery("SELECT count(*) FROM TEST.NOT_NULLS");
    ) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    assertEquals(2, targetRunner.getErrorRecords().size());
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
        ChangeLogFormat.NONE,
        JDBCOperationType.INSERT,
        UnsupportedOperationAction.DISCARD,
        createConfigBean(h2ConnectionString, username, password),
        Collections.emptyList()
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
