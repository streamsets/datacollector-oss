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
package com.streamsets.pipeline.stage.origin.binlog;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.config.MysqlBinLogSourceConfig;
import com.streamsets.pipeline.stage.origin.mysql.binlog.MysqlBinLogSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsEmptyCollection;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.streamsets.pipeline.api.Field.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

public abstract class TestAbstractMysqlSource {
  public static final int MAX_BATCH_SIZE = 500;
  public static final int SERVER_ID = 999;
  public static final String LANE = "lane";
  private static final Integer MYSQL_PORT = 3306;
  private static final String MYSQL_PASSWORD = "test";

  protected static GenericContainer mysql;

  protected static HikariDataSource ds;
  protected SourceRunner runner;

  @After
  public void teardown() throws Exception {
    for (String table: Arrays.asList("foo", "foo2", "ALL_TYPES")) {
      if (tableExists(table)) {
        execute(ds, String.format("TRUNCATE %s", table));
      }
    }
    if (runner != null) {
      try {
        runner.runDestroy();
      } catch (Exception ignored) {
        // just don't care. runner may have not even been initialized
      }
    }
  }

  @Test
  public void shouldFailWhenUserIsNotSuper() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("test");
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertThat(issues, hasSize(1));
  }

  @Test
  public void shouldFailWhenUCannotConnect() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    config.port = 1;
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertThat(issues, hasSize(2));
  }

  @Test
  public void shouldConvertAllMysqlTypes() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(empty()));

    // spec timezone in timestamp
    String sql = "INSERT INTO ALL_TYPES VALUES (\n" +
        "    1,\n" +
        "    2,\n" +
        "    3,\n" +
        "    4,\n" +
        "    5.1,\n" +
        "    6.1,\n" +
        "    '2016-08-18 12:01:02',\n" +
        "    7,\n" +
        "    8,\n" +
        "    '2016-08-18',\n" +
        "    '12:01:02',\n" +
        "    '2016-08-18 12:01:02',\n" +
        "    2016,\n" +
        "    'A',\n" +
        "    'a',\n" +
        "    '1',\n" +
        "    '1',\n" +
        "    '2',\n" +
        "    '3',\n" +
        "    '4',\n" +
        "    'text',\n" +
        "    'text2',\n" +
        "    'text3',\n" +
        "    null\n" +
        ")";

    execute(ds, sql);

    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), hasSize(1));

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
    DateTimeFormatter formatterDate = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();
    DateTimeFormatter formatterTime = DateTimeFormat.forPattern("HH:mm:ss").withZoneUTC();
    Record rec = output.getRecords().get(LANE).get(0);
    assertThat(rec.get("/Data/_decimal"), is(create(BigDecimal.ONE)));
    assertThat(rec.get("/Data/_tinyint"), is(create(2)));
    assertThat(rec.get("/Data/_smallint"), is(create(3)));
    assertThat(rec.get("/Data/_mediumint"), is(create(4)));
    assertThat(rec.get("/Data/_float"), is(create((5.1f))));
    assertThat(rec.get("/Data/_double"), is(create(6.1)));

    assertThat(rec.get("/Data/_timestamp"), is(createDatetime(
        formatter.parseDateTime("2016-08-18 12:01:02").toDate()
    )));

    assertThat(rec.get("/Data/_date"), is(createDate(
        formatterDate.parseDateTime("2016-08-18").toDate()
    )));

    assertThat(rec.get("/Data/_time"), is(createTime(
        formatterTime.parseDateTime("12:01:02").toDate()
    )));

    assertThat(rec.get("/Data/_datetime"), is(createDatetime(
        formatter.parseDateTime("2016-08-18 12:01:02").toDate()
    )));

    assertThat(rec.get("/Data/_bigint"), is(create(7L)));
    assertThat(rec.get("/Data/_int"), is(create(8)));
    assertThat(rec.get("/Data/_year"), is(create(2016)));
    assertThat(rec.get("/Data/_varchar"), is(create("A")));
    assertThat(rec.get("/Data/_enum"), is(create(1)));
    assertThat(rec.get("/Data/_set"), is(create(1L)));
    assertThat(rec.get("/Data/_tinyblob"), is(create("1".getBytes())));
    assertThat(rec.get("/Data/_mediumblob"), is(create("2".getBytes())));
    assertThat(rec.get("/Data/_longblob"), is(create("3".getBytes())));
    assertThat(rec.get("/Data/_blob"), is(create("4".getBytes())));
    assertThat(rec.get("/Data/_text"), is(create("text")));
    assertThat(rec.get("/Data/_tinytext"), is(create("text2")));
    assertThat(rec.get("/Data/_mediumtext"), is(create("text3")));
    assertThat(rec.get("/Data/_longtext"), is(create((String) null)));

    // test header
    assertThat(rec.get("/Database"), is(create("test")));
    assertThat(rec.get("/Table"), is(create("ALL_TYPES")));
    assertThat(rec.get("/ServerId"), notNullValue());
    assertThat(rec.get("/Timestamp"), notNullValue());
    assertThat(rec.get("/Type"), is(create("INSERT")));
  }

  @Test
  public void shouldStartFromBeginning() throws Exception {
    execute(ds, "INSERT INTO foo (bar) VALUES (1)");

    MysqlBinLogSourceConfig config = createConfig("root");
    config.startFromBeginning = true;
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    List<Record> records = new ArrayList<>();

    while (!output.getRecords().get(LANE).isEmpty()) {
      records.addAll(output.getRecords().get(LANE));
      output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    }

    Record found = null;
    for (Record record : records) {
      if (record.get("/Table").getValueAsString().equals("foo")) {
        found = record;
        break;
      }
    }
    assertThat(found, notNullValue());
  }

  @Test
  public void shouldStartFromCurrent() throws Exception {
    execute(ds, "INSERT INTO foo (bar) VALUES (1)");

    MysqlBinLogSourceConfig config = createConfig("root");
    config.startFromBeginning = false;
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    List<Record> records = new ArrayList<>();

    while (!output.getRecords().get(LANE).isEmpty()) {
      records.addAll(output.getRecords().get(LANE));
      output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    }
    assertThat(records, is(IsEmptyCollection.<Record>empty()));

    // add one more
    execute(ds, "INSERT INTO foo (bar) VALUES (2)");
    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    records.addAll(output.getRecords().get(LANE));

    Record found = null;
    for (Record record : records) {
      if (record.get("/Table").getValueAsString().equals("foo")) {
        found = record;
        break;
      }
    }
    assertThat(found, notNullValue());
    assertThat(found.get("/Data/bar"), is(create(2)));
  }

  @Test
  public void shouldCreateMutipleRecordsForEventWithMultipleRows() throws Exception {
    int count = 10;
    for (int i = 0; i < count; i++) {
      execute(ds, String.format("INSERT INTO foo (bar) VALUES (%d)", i));
    }

    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));

    execute(ds, "DELETE FROM foo");

    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);

    List<Record> records = new ArrayList<>();
    records.addAll(output.getRecords().get(LANE));

    assertThat(records, hasSize(count));
    for (int i = 0; i < count; i++) {
      Record rec = records.get(i);
      assertThat(rec.get("/Type"), is(create("DELETE")));
      assertThat(rec.get("/OldData/bar"), is(create(i)));
    }

    // no more data
    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));

    // no more data after reconnect
    runner.runDestroy();
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();
    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));
  }

  @Test
  public void shouldSendAllEventRecordsDiscardingbatchSize() throws Exception {
    int count = 100;
    for (int i = 0; i < count; i++) {
      execute(ds, String.format("INSERT INTO foo (bar) VALUES (%d)", i));
    }

    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, count);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));

    execute(ds, "DELETE FROM foo");

    output = runner.runProduce(output.getNewOffset(), count / 2);

    List<Record> records = new ArrayList<>();
    records.addAll(output.getRecords().get(LANE));

    assertThat(records, hasSize(count));
  }

  @Test
  public void shouldHandlePartialUpdates() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    StageRunner.Output output = runner.runProduce(null, MAX_BATCH_SIZE);
    List<Record> records = new ArrayList<>(output.getRecords().get(LANE));
    assertThat(records, is(Matchers.<Record>empty()));

    // add one more
    execute(ds, "INSERT INTO foo2 (a, b) VALUES (1, 2)");
    output = runner.runProduce(null, MAX_BATCH_SIZE);
    records = new ArrayList<>(output.getRecords().get(LANE));
    assertThat(records, hasSize(1));

    Record rec = records.get(0);
    assertThat(rec.get("/Data/a"), is(create(1)));
    assertThat(rec.get("/Data/b"), is(create(2)));
    assertThat(rec.get("/Data/c"), is(create(3)));

    execute(ds, "UPDATE foo2 set a = 11, c = 33");
    output = runner.runProduce(null, MAX_BATCH_SIZE);
    records = new ArrayList<>(output.getRecords().get(LANE));
    rec = records.get(0);
    assertThat(records, hasSize(1));

    assertThat(rec.get("/Data/a"), is(create(11)));
    assertThat(rec.get("/Data/b"), is(create(2)));
    assertThat(rec.get("/Data/c"), is(create(33)));

    assertThat(rec.get("/OldData/a"), is(create(1)));
    assertThat(rec.get("/OldData/b"), is(create(2)));
    assertThat(rec.get("/OldData/c"), is(create(3)));
  }

  @Test
  public void shouldIncludeAndIgnoreTables() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    config.includeTables = "test.foo,t%.foo2";
    config.ignoreTables = "test.foo";
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));

    execute(ds, "INSERT INTO foo (bar) VALUES (1)");
    execute(ds, "INSERT INTO foo2 VALUES (1, 2, 3)");

    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    List<Record> records = output.getRecords().get(LANE);
    assertThat(records, hasSize(1));
    assertThat(records.get(0).get("/Table").getValueAsString(), is("foo2"));
    execute(ds, "TRUNCATE foo");
    execute(ds, "TRUNCATE foo2");
  }

  @Test
  public void shouldIgnoreEmptyFilters() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    config.includeTables = "";
    config.ignoreTables = "";
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));

    execute(ds, "INSERT INTO foo (bar) VALUES (1)");
    execute(ds, "INSERT INTO foo2 VALUES (1, 2, 3)");

    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    List<Record> records = output.getRecords().get(LANE);
    assertThat(records, hasSize(2));
  }

  @Test
  public void shouldReturnCorrectOffsetForFilteredOutEvents() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    config.ignoreTables = "test.foo";
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    assertThat(output.getRecords().get(LANE), is(IsEmptyCollection.<Record>empty()));

    execute(ds, "INSERT INTO foo (bar) VALUES (1)");

    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    List<Record> records = output.getRecords().get(LANE);
    assertThat(records, is(empty()));
    assertThat(output.getNewOffset(), not(isEmptyString()));
  }

  @Test
  public void shouldCreateRecordWithoutColumnNamesWhenMetadataNotFound() throws Exception {
    MysqlBinLogSourceConfig config = createConfig("root");
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    String offset = output.getNewOffset();

    String sql = "INSERT INTO ALL_TYPES VALUES (\n" +
        "    1,\n" +
        "    2,\n" +
        "    3,\n" +
        "    4,\n" +
        "    5.1,\n" +
        "    6.1,\n" +
        "    '2016-08-18 12:01:02',\n" +
        "    7,\n" +
        "    8,\n" +
        "    '2016-08-18',\n" +
        "    '12:01:02',\n" +
        "    '2016-08-18 12:01:02',\n" +
        "    2016,\n" +
        "    'A',\n" +
        "    'a',\n" +
        "    '1',\n" +
        "    '1',\n" +
        "    '2',\n" +
        "    '3',\n" +
        "    '4',\n" +
        "    'text',\n" +
        "    'text2',\n" +
        "    'text3',\n" +
        "    null\n" +
        ")";

    execute(ds, sql);
    execute(ds, "DROP TABLE ALL_TYPES");

    // this should not fail due to onError policy
    output = runner.runProduce(offset, MAX_BATCH_SIZE);

    assertThat(output.getRecords().get(LANE), hasSize(1));

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
    DateTimeFormatter formatterDateLocal = DateTimeFormat.forPattern("yyyy-MM-dd");
    Record rec = output.getRecords().get(LANE).get(0);
    assertThat(rec.get("/Data/col_0"), is(create("1")));
    assertThat(rec.get("/Data/col_1"), is(create("2")));
    assertThat(rec.get("/Data/col_2"), is(create("3")));
    assertThat(rec.get("/Data/col_3"), is(create("4")));
    assertThat(rec.get("/Data/col_4"), is(create(("5.1"))));
    assertThat(rec.get("/Data/col_5"), is(create("6.1")));

    assertThat(rec.get("/Data/col_6"), is(create(
        new java.sql.Timestamp(
            formatter.parseDateTime("2016-08-18 12:01:02").getMillis()
        ).toString()
    )));

    assertThat(rec.get("/Data/col_7"), is(create("7")));
    assertThat(rec.get("/Data/col_8"), is(create("8")));

    assertThat(rec.get("/Data/col_9"), is(create(
        formatterDateLocal.print(
            formatter.parseDateTime("2016-08-18 12:01:02").withTimeAtStartOfDay().getMillis()
        )
    )));

    assertThat(rec.get("/Data/col_11"), is(create(
        formatter.parseDateTime("2016-08-18 12:01:02").toDate().toString()
    )));

    assertThat(rec.get("/Data/col_12"), is(create("2016")));
    assertThat(rec.get("/Data/col_13"), is(create("A")));
    assertThat(rec.get("/Data/col_14"), is(create("1")));
    assertThat(rec.get("/Data/col_15"), is(create("1")));
    assertThat(rec.get("/Data/col_16"), is(create("1")));
    assertThat(rec.get("/Data/col_17"), is(create("2")));
    assertThat(rec.get("/Data/col_18"), is(create("3")));
    assertThat(rec.get("/Data/col_19"), is(create("4")));
    assertThat(rec.get("/Data/col_20"), is(create("text")));
    assertThat(rec.get("/Data/col_21"), is(create("text2")));
    assertThat(rec.get("/Data/col_22"), is(create("text3")));
    assertThat(rec.get("/Data/col_23"), is(create((String) null)));

    // test header
    assertThat(rec.get("/Database"), is(create("test")));
    assertThat(rec.get("/Table"), is(create("ALL_TYPES")));
    assertThat(rec.get("/ServerId"), notNullValue());
    assertThat(rec.get("/Timestamp"), notNullValue());
    assertThat(rec.get("/Type"), is(create("INSERT")));
  }

  @Test
  public void shouldSetClientServerId() throws Exception {
    // get current offset
    MysqlBinLogSourceConfig tconfig = createConfig("root");
    tconfig.startFromBeginning = true;
    MysqlBinLogSource tsource = createMysqlSource(tconfig);
    SourceRunner trunner = new SourceRunner.Builder(MySQLBinLogDSource.class, tsource)
        .addOutputLane(LANE)
        .build();
    StageRunner.Output toutput = null;
    try {
      trunner.runInit();
      toutput = trunner.runProduce(null, MAX_BATCH_SIZE);
      while (!toutput.getRecords().get(LANE).isEmpty()) {
        toutput = trunner.runProduce(toutput.getNewOffset(), MAX_BATCH_SIZE);
      }
    } finally {
      trunner.runDestroy();
    }
    final String lastSourceOffset = toutput.getNewOffset();

    // now start two sources from same offset
    execute(ds, "INSERT INTO foo (bar) VALUES (1)");

    List<Record> records = new Vector<>();

    List<String> serverIds = Arrays.asList("1", "2");
    List<SourceRunner> runners = new ArrayList<>();

    MysqlBinLogSourceConfig config = createConfig("root");
    config.startFromBeginning = false;
    MysqlBinLogSource source = createMysqlSource(config);
    runner = new SourceRunner.Builder(MySQLBinLogDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();
    runners.add(runner);

    ExecutorService ec = Executors.newFixedThreadPool(2);
    CountDownLatch latch = new CountDownLatch(2);

    for (final SourceRunner runner : runners) {
      ec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);

            while (!output.getRecords().get(LANE).isEmpty()) {
              records.addAll(output.getRecords().get(LANE));
              output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          } finally {
            latch.countDown();
            try {
              runner.runDestroy();
            } catch (StageException e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    latch.await();

    int count = 0;
    for (Record record : records) {
      if (record.get("/Table").getValueAsString().equals("foo")) {
        count += 1;
      }
    }
    assertThat(count, is(2));
  }

  protected void execute(DataSource ds, String sql) throws SQLException {
    execute(ds, Collections.singletonList(sql));
  }

  protected void execute(DataSource ds, List<String> sql) throws SQLException {
    try (Connection conn = ds.getConnection()) {
      for (String s : sql) {
        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate(s);
          stmt.close();
        }
      }
      conn.commit();
    }
  }

  private static String getJdbcUrl(){
    return"jdbc:mysql://" + mysql.getContainerIpAddress() +  ":" + mysql.getMappedPort(MYSQL_PORT) + "/test";
  }

  protected static HikariDataSource connect() {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(getJdbcUrl());
    hikariConfig.setUsername("root");
    hikariConfig.setPassword(MYSQL_PASSWORD);
    hikariConfig.setAutoCommit(false);
    return new HikariDataSource(hikariConfig);
  }

  protected MysqlBinLogSourceConfig createConfig(String username) {
    MysqlBinLogSourceConfig config = new MysqlBinLogSourceConfig();
    config.username = () -> username;
    config.password = () -> MYSQL_PASSWORD;
    config.serverId = SERVER_ID;
    config.maxWaitTime = 1000;
    config.maxBatchSize = 1000;
    config.connectionTimeout = 5000;
    config.startFromBeginning = false;
    return config;
  }

  protected MysqlBinLogSource createMysqlSource(final MysqlBinLogSourceConfig config) {
    return new MysqlBinLogSource() {
      @Override
      public MysqlBinLogSourceConfig getConfig() {
        return config;
      }
    };
  }

  protected String getBinlogFilename() throws SQLException {
    return getMasterStatus().get("File").toString();
  }

  protected long getBinlogPosition() throws SQLException {
    return Long.parseLong(getMasterStatus().get("Position").toString());
  }

  protected Map<String, Object> getMasterStatus() throws SQLException {
    Map<String, Object> res = new HashMap<>();
    try (Connection conn = ds.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("show master status");
        rs.next();
        res.put("File", rs.getString("File"));
        res.put("Position", rs.getString("Position"));
        res.put("Executed_Gtid_Set", rs.getString("Executed_Gtid_Set"));
      }
    }
    return res;
  }

  private boolean tableExists(String tableName) throws SQLException {
    try (Connection conn = ds.getConnection()) {
      try (
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(
              String.format("select * from information_schema.tables where table_schema = 'test' and table_name = '%s' limit 1", tableName)
          )
      ) {
        return rs.next();
      }
    }
  }
}
