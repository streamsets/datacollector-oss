/**
 * Copyright 2015 StreamSets Inc.
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


import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcFieldMappingConfig;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestJdbcMultiRowRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestMicrosoftChangeLogWriter.class);

  private final String username = "sa";
  private final String password = "sa";
  private static final String connectionString = "jdbc:h2:mem:test";
  private DataSource dataSource;
  private Connection connection;

  private long id;
  private static final Random random = new Random();

  @Before
  public void setUp() throws Exception {
    id = 0L;
    // Create a table in H2 and put some data in it for querying.
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionString);
    config.setUsername(username);
    config.setPassword(password);
    config.setMaximumPoolSize(3);
    dataSource = new HikariDataSource(config);

    connection = dataSource.getConnection();
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
              "(P_ID INT NOT NULL, F1 INT, F2 INT, F3 INT, UNIQUE(P_ID), PRIMARY KEY(P_ID));"
      );
      String unprivUser = "unpriv_user";
      String unprivPassword = "unpriv_pass";
      statement.addBatch("CREATE USER IF NOT EXISTS " + unprivUser + " PASSWORD '" + unprivPassword + "';");
      statement.addBatch("GRANT SELECT ON TEST.TEST_TABLE TO " + unprivUser + ";");

      statement.executeBatch();
    }
  }

  @After
  public void tearDown() throws Exception {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TABLE;");
    }

    // Last open connection terminates H2
    connection.close();
  }

  @Test
  public void testThreePartitions() throws Exception {
    List<JdbcFieldMappingConfig> mappings = new ArrayList<>();

    JdbcRecordWriter writer = new JdbcMultiRowRecordWriter(
        connectionString,
        dataSource,
        "TEST.TEST_TABLE",
        false,
        mappings,
        JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS);
    List<Record> batch = generateRecords(10);
    writer.writeBatch(batch);

    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(10, rs.getInt(1));
    }
  }

  @Test
  public void testParameterLimit() throws Exception {
    List<JdbcFieldMappingConfig> mappings = new ArrayList<>();

    JdbcRecordWriter writer = new JdbcMultiRowRecordWriter(
        connectionString,
        dataSource,
        "TEST.TEST_TABLE",
        false,
        mappings,
        8);

    Collection<Record> records = generateRecords(10);
    writer.writeBatch(records);


    // note that unfortunately we have no way to directly observe that the expected batching is actually occurring -
    // only that the correct number of rows comes out even when we enable a meaningful parameter limit.
    connection = DriverManager.getConnection(connectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST.TEST_TABLE");
      rs.next();
      assertEquals(10, rs.getInt(1));
    }
  }

  private List<Record> generateRecords(int numRecords) {
    List<Record> records = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      records.add(generateRecord());
    }
    return records;
  }

  private Record generateRecord() {
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("P_ID", Field.create(++id));
    fields.put("F1", Field.create(random.nextInt()));
    if (id % 2 == 0) {
      fields.put("F2", Field.create(random.nextInt()));
    }
    if (id % 4 == 0) {
      fields.put("F3", Field.create(random.nextInt()));
    }
    record.set(Field.create(fields));

    LOG.debug(record.toString());
    return record;
  }
}
