/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestJdbcTarget {

  private final String username = "sa";
  private final String password = "sa";
  private final String unprivUser = "unpriv_user";
  private final String unprivPassword = "unpriv_pass";
  private final String database = "TEST";
  private final String tableName = "TEST.TEST_TABLE";

  private final String h2ConnectionString = "jdbc:h2:mem:" + database;

  private Connection connection = null;

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
          "(p_id INT NOT NULL, first_name VARCHAR(255), last_name VARCHAR(255), ts TIMESTAMP, UNIQUE(p_id));"
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
    }

    // Last open connection terminates H2
    connection.close();
  }

  @Test
  public void testEmptyBatch() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", username)
        .addConfiguration("password", password)
        .build();

    List<Record> emptyBatch = ImmutableList.of();
    targetRunner.runInit();
    targetRunner.runWrite(emptyBatch);
    targetRunner.runDestroy();
  }

  @Test
  public void testSingleRecord() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", username)
        .addConfiguration("password", password)
        .build();

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
  public void testRecordWithBatchUpdateException() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", username)
        .addConfiguration("password", password)
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
  public void testRecordWithDataTypeException() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", username)
        .addConfiguration("password", password)
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
    fields2.add(Field.create("Natkins"));
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
  public void testRecordWithBadPermissions() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", unprivUser)
        .addConfiguration("password", unprivPassword)
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

    assertEquals(3, targetRunner.getErrorRecords().size());
  }

  @Test
  public void testBadConnectionString() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .addConfiguration("connectionString", "bad connection string")
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", username)
        .addConfiguration("password", password)
        .build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadCredentials() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first_name"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", "foo")
        .addConfiguration("password", "bar")
        .build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadColumnMapping() throws Exception {
    List<JdbcFieldMappingConfig> fieldMappings = ImmutableList.of(
        new JdbcFieldMappingConfig("[0]", "p_id"),
        new JdbcFieldMappingConfig("[1]", "first"),
        new JdbcFieldMappingConfig("[2]", "last_name"),
        new JdbcFieldMappingConfig("[3]", "ts")
    );

    TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class)
        .addConfiguration("connectionString", h2ConnectionString)
        .addConfiguration("useCredentials", true)
        .addConfiguration("rollbackOnError", false)
        .addConfiguration("qualifiedTableName", tableName)
        .addConfiguration("columnNames", fieldMappings)
        .addConfiguration("username", username)
        .addConfiguration("password", password)
        .build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }
}
