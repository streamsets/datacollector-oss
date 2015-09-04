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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMicrosoftChangeLogWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestMicrosoftChangeLogWriter.class);

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
              "(P_ID INT NOT NULL, MSG VARCHAR(255), UNIQUE(P_ID), PRIMARY KEY(P_ID));"
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
    fields.put(MicrosoftJdbcRecordWriter.OP_FIELD.substring(1), Field.create(2));
    fields.put("P_ID", Field.create(100));
    fields.put("MSG", Field.create("unit test"));
    record.set(Field.create(fields));

    MicrosoftJdbcRecordWriter writer = new MicrosoftJdbcRecordWriter(connectionString, dataSource, "TEST.TEST_TABLE");
    Batch batch = new BatchImpl("test", "0", ImmutableList.of(record));
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
    insert.put(MicrosoftJdbcRecordWriter.OP_FIELD.substring(1), Field.create(2));
    insert.put("P_ID", Field.create(200));
    insert.put("MSG", Field.create("first message"));
    insertRecord.set(Field.create(insert));

    Record updateRecord = RecordCreator.create();
    Map<String, Field> update = new HashMap<>();
    update.put(MicrosoftJdbcRecordWriter.OP_FIELD.substring(1), Field.create(4));
    update.put("P_ID", Field.create(200));
    update.put("MSG", Field.create("second message"));
    updateRecord.set(Field.create(update));

    MicrosoftJdbcRecordWriter writer = new MicrosoftJdbcRecordWriter(connectionString, dataSource, "TEST.TEST_TABLE");
    Batch batch = new BatchImpl("test", "0", ImmutableList.of(insertRecord, updateRecord));
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
    insert.put(MicrosoftJdbcRecordWriter.OP_FIELD.substring(1), Field.create(2));
    insert.put("P_ID", Field.create(300));
    insert.put("MSG", Field.create("message"));
    insertRecord.set(Field.create(insert));

    Record deleteRecord = RecordCreator.create();
    Map<String, Field> delete = new HashMap<>();
    delete.put(MicrosoftJdbcRecordWriter.OP_FIELD.substring(1), Field.create(1));
    delete.put("P_ID", Field.create(300));
    delete.put("MSG", Field.create("message"));
    deleteRecord.set(Field.create(delete));

    MicrosoftJdbcRecordWriter writer = new MicrosoftJdbcRecordWriter(connectionString, dataSource, "TEST.TEST_TABLE");
    Batch batch = new BatchImpl("test", "0", ImmutableList.of(insertRecord, deleteRecord));
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(0, rs.getInt(1));
    }
  }
}
