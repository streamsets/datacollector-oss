/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BasicIT extends BaseTableJdbcSourceIT {
  private static final String SPORTS_STARS_INSERT_TEMPLATE = "INSERT into TEST.%s values (%s, '%s', '%s');";

  private static List<Record> EXPECTED_CRICKET_STARS_RECORDS;
  private static List<Record> EXPECTED_TENNIS_STARS_RECORDS;

  private static Record createSportsStarsRecords(int pid, String first_name, String last_name) {
    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("p_id", Field.create(pid));
    fields.put("first_name", Field.create(first_name));
    fields.put("last_name", Field.create(last_name));
    record.set(Field.createListMap(fields));
    return record;
  }

  @BeforeClass
  public static void setupTables() throws SQLException {
    EXPECTED_CRICKET_STARS_RECORDS = ImmutableList.of(
        createSportsStarsRecords(1, "Sachin", "Tendulkar"),
        createSportsStarsRecords(2, "Mahendra Singh", "Dhoni"),
        createSportsStarsRecords(3, "Don", "Bradman"),
        createSportsStarsRecords(4, "Brian", "Lara"),
        createSportsStarsRecords(5, "Allan", "Border"),
        createSportsStarsRecords(6, "Clive", "Lloyd"),
        createSportsStarsRecords(7, "Richard", "Hadlee"),
        createSportsStarsRecords(8, "Richie", "Benaud"),
        createSportsStarsRecords(9, "Sunil", "Gavaskar"),
        createSportsStarsRecords(10, "Shane", "Warne")
    );

    EXPECTED_TENNIS_STARS_RECORDS = ImmutableList.of(
        createSportsStarsRecords(1, "Novak", "Djokovic"),
        createSportsStarsRecords(2, "Andy", "Murray"),
        createSportsStarsRecords(3, "Stan", "Wawrinka"),
        createSportsStarsRecords(4, "Milos", "Raonic"),
        createSportsStarsRecords(5, "Kei", "Nishikori"),
        createSportsStarsRecords(6, "Rafael", "Nadal"),
        createSportsStarsRecords(7, "Roger", "Federer"),
        createSportsStarsRecords(8, "Dominic", "Thiem"),
        createSportsStarsRecords(9, "Tomas", "Berdych"),
        createSportsStarsRecords(10, "David", "Goffin"),
        createSportsStarsRecords(11, "Marin", "Cilic"),
        createSportsStarsRecords(12, "Gael", "Monfils"),
        createSportsStarsRecords(13, "Nick", "Kyrgios"),
        createSportsStarsRecords(14, "Roberto Bautista", "Agut"),
        createSportsStarsRecords(15, "Jo-Wilfried", "Tsonga")
    );

    try (Statement statement = connection.createStatement()) {
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");

      //CRICKET_STARS
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.CRICKET_STARS " +
              "(p_id INT NOT NULL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255));"
      );

      for (Record record : EXPECTED_CRICKET_STARS_RECORDS) {
        statement.addBatch(
            String.format(
                SPORTS_STARS_INSERT_TEMPLATE,
                "CRICKET_STARS",
                record.get("/p_id").getValueAsInteger(),
                record.get("/first_name").getValueAsString(),
                record.get("/last_name").getValueAsString()
            )
        );
      }

      //TENNIS STARS
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TENNIS_STARS " +
              "(p_id INT NOT NULL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255));"
      );

      for (Record record : EXPECTED_TENNIS_STARS_RECORDS) {
        statement.addBatch(
            String.format(
                SPORTS_STARS_INSERT_TEMPLATE,
                "TENNIS_STARS",
                record.get("/p_id").getValueAsInteger(),
                record.get("/first_name").getValueAsString(),
                record.get("/last_name").getValueAsString()
            )
        );
      }

      statement.executeBatch();
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("DROP TABLE IF EXISTS TEST.CRICKET_STARS;");
      statement.execute("DROP TABLE IF EXISTS TEST.TENNIS_STARS;");
    }
  }

  @Test
  public void testNoTableMatchesTablePatternValidationError() throws Exception {
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "NO_TABLE%";
    tableConfigBean.schema = database;

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        TestTableJdbcSource.createHikariPoolConfigBean(JDBC_URL, USER_NAME, PASSWORD),
        TestTableJdbcSource.createCommonSourceConfigBean(1, 1000, 1000, 1000),
        TestTableJdbcSource.createTableJdbcConfigBean(ImmutableList.of(tableConfigBean), false, -1, TableOrderStrategy.NONE)
    );

    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testSingleTableSingleBatch() throws Exception {
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "CRICKET_STARS";
    tableConfigBean.schema = database;

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        TestTableJdbcSource.createHikariPoolConfigBean(JDBC_URL, USER_NAME, PASSWORD),
        TestTableJdbcSource.createCommonSourceConfigBean(1, 1000, 1000, 1000),
        TestTableJdbcSource.createTableJdbcConfigBean(ImmutableList.of(tableConfigBean), false, -1, TableOrderStrategy.NONE)
    );

    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("", 1000);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(10, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS, records);
      Assert.assertEquals(0, runner.runProduce(output.getNewOffset(), 1000).getRecords().get("a").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSingleTableMultipleBatches() throws Exception {
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "CRICKET_STARS";
    tableConfigBean.schema = database;

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        TestTableJdbcSource.createHikariPoolConfigBean(JDBC_URL, USER_NAME, PASSWORD),
        TestTableJdbcSource.createCommonSourceConfigBean(1, 1000, 1000, 1000),
        TestTableJdbcSource.createTableJdbcConfigBean(ImmutableList.of(tableConfigBean), false, -1, TableOrderStrategy.NONE)
    );
    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("", 5);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(0, 5), records);

      output = runner.runProduce(output.getNewOffset(), 5);
      records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(5, 10), records);

      Assert.assertEquals(0, runner.runProduce(output.getNewOffset(), 1000).getRecords().get("a").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleTablesSingleBatch() throws Exception {
    TableConfigBean tableConfigBean1 = new TableConfigBean();
    tableConfigBean1.tablePattern = "CRICKET_STARS";
    tableConfigBean1.schema = database;

    TableConfigBean tableConfigBean2 = new TableConfigBean();
    tableConfigBean2.tablePattern = "TENNIS_STARS";
    tableConfigBean2.schema = database;

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        TestTableJdbcSource.createHikariPoolConfigBean(JDBC_URL, USER_NAME, PASSWORD),
        TestTableJdbcSource.createCommonSourceConfigBean(1, 1000, 1000, 1000),
        TestTableJdbcSource.createTableJdbcConfigBean(ImmutableList.of(tableConfigBean1, tableConfigBean2), false, -1, TableOrderStrategy.NONE)
    );

    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("", 1000);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(10, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS, records);

      output = runner.runProduce(output.getNewOffset(), 1000);
      records = output.getRecords().get("a");
      Assert.assertEquals(15, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS, records);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleTablesMultipleBatches() throws Exception {
    //With a '%' regex which has to select both tables.
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "%";
    tableConfigBean.schema = database;

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        TestTableJdbcSource.createHikariPoolConfigBean(JDBC_URL, USER_NAME, PASSWORD),
        TestTableJdbcSource.createCommonSourceConfigBean(1, 1000, 1000, 1000),
        TestTableJdbcSource.createTableJdbcConfigBean(ImmutableList.of(tableConfigBean), false, -1, TableOrderStrategy.NONE)
    );

    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("", 5);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(0, 5), records);

      output = runner.runProduce(output.getNewOffset(), 5);
      records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(0, 5), records);

      output = runner.runProduce(output.getNewOffset(), 5);
      records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(5, 10), records);

      output = runner.runProduce(output.getNewOffset(), 5);
      records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(5, 10), records);

      output = runner.runProduce(output.getNewOffset(), 5);
      records = output.getRecords().get("a");
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(10, 15), records);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMetrics() throws Exception {
    //With a '%' regex which has to select both tables.
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "%";
    tableConfigBean.schema = database;

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        TestTableJdbcSource.createHikariPoolConfigBean(JDBC_URL, USER_NAME, PASSWORD),
        TestTableJdbcSource.createCommonSourceConfigBean(1, 1000, 1000, 1000),
        TestTableJdbcSource.createTableJdbcConfigBean(ImmutableList.of(tableConfigBean), false, -1, TableOrderStrategy.NONE)
    );

    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    Stage.Context context = runner.getContext();
    try {
      Map<String, Object> gaugeMap = (Map<String, Object>)context.getGauge(TableJdbcSource.TABLE_METRICS).getValue();
      Integer numberOfTables = (int)gaugeMap.get(TableJdbcSource.TABLE_COUNT);
      Assert.assertEquals(2, numberOfTables.intValue());

      StageRunner.Output output = runner.runProduce("", 1000);
      Assert.assertEquals("TEST.CRICKET_STARS", gaugeMap.get(TableJdbcSource.CURRENT_TABLE));

      runner.runProduce(output.getNewOffset(), 1000);
      Assert.assertEquals("TEST.TENNIS_STARS", gaugeMap.get(TableJdbcSource.CURRENT_TABLE));
    } finally {
      runner.runDestroy();
    }
  }
}
