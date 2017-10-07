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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.MultithreadedTableProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableJdbcRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.util.OffsetUtil;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BasicIT extends BaseTableJdbcSourceIT {
  private static final String STARS_INSERT_TEMPLATE = "INSERT into TEST.%s values (%s, '%s', '%s')";
  private static final String TRANSACTION_INSERT_TEMPLATE = "INSERT into TEST.%s values (%s, %s, '%s')";
  private static final String OTHER_TABLE_INSERT_TEMPLATE = "INSERT into `TEST`.%s values (%s, '%s')";
  private static final String OTHER_TABLE_CREATE_TEMPLATE =
      "CREATE TABLE `TEST`.%s(unique_int INT NOT NULL PRIMARY KEY, random_string VARCHAR(255))";

  private static final String QUOTED_TABLE_NAME_WITHOUT_QUOTES = "05CDB2D7-6B8C-42F2-A6A6-041E123883EE";
  private static final String QUOTED_TABLE_NAME = "`" + QUOTED_TABLE_NAME_WITHOUT_QUOTES + "`";

  private static List<Record> EXPECTED_CRICKET_STARS_RECORDS;
  private static List<Record> EXPECTED_TENNIS_STARS_RECORDS;
  private static List<Record> EXPECTED_TRANSACTION_RECORDS;
  private static List<Record> EXPECTED_QUOTE_TESTING_RECORDS;


  private static Record createSportsStarsRecords(int pid, String first_name, String last_name) {
    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("p_id", Field.create(pid));
    fields.put("first_name", Field.create(first_name));
    fields.put("last_name", Field.create(last_name));
    record.set(Field.createListMap(fields));
    return record;
  }

  private static List<Record> createTransactionRecords(int noOfRecords) {
    List<Record> records = new ArrayList<>();
    long currentTime = (System.currentTimeMillis() / 1000) * 1000;
    for (int i = 0; i < noOfRecords; i++) {
      Record record = RecordCreator.create();
      LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
      fields.put("unique_int", Field.create(i + 1));
      fields.put("t_date", Field.create(Field.Type.LONG, currentTime));
      fields.put("random_string", Field.create(UUID.randomUUID().toString()));
      record.set(Field.createListMap(fields));
      records.add(record);
      //making sure time is at least off by a second.
      currentTime = currentTime + 1000;
    }
    return records;
  }

  private static Record createOtherTableRecord(int index) {
    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("unique_int", Field.create(Field.Type.INTEGER, index + 1));
    fields.put("random_string", Field.create(UUID.randomUUID().toString()));
    record.set(Field.createListMap(fields));
    return record;
  }

  @BeforeClass
  public static void setupTables() throws SQLException {
    DataCollectorServicesUtils.loadDefaultServices();

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

    EXPECTED_TRANSACTION_RECORDS = createTransactionRecords(20);

    EXPECTED_QUOTE_TESTING_RECORDS = IntStream.range(0, 20).mapToObj(BasicIT::createOtherTableRecord).collect(Collectors.toList());

    try (Statement statement = connection.createStatement()) {
      //CRICKET_STARS
      statement.addBatch(
          "CREATE TABLE TEST.CRICKET_STARS " +
              "(p_id INT NOT NULL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))"
      );

      for (Record record : EXPECTED_CRICKET_STARS_RECORDS) {
        statement.addBatch(
            String.format(
                STARS_INSERT_TEMPLATE,
                "CRICKET_STARS",
                record.get("/p_id").getValueAsInteger(),
                record.get("/first_name").getValueAsString(),
                record.get("/last_name").getValueAsString()
            )
        );
      }

      // VIEW for CRICKET_STARTS
      statement.addBatch("CREATE VIEW TEST.CRICKET_STARS_VIEW AS SELECT * FROM TEST.CRICKET_STARS");

      //TENNIS STARS
      statement.addBatch(
          "CREATE TABLE TEST.TENNIS_STARS " +
              "(p_id INT NOT NULL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))"
      );

      for (Record record : EXPECTED_TENNIS_STARS_RECORDS) {
        statement.addBatch(
            String.format(
                STARS_INSERT_TEMPLATE,
                "TENNIS_STARS",
                record.get("/p_id").getValueAsInteger(),
                record.get("/first_name").getValueAsString(),
                record.get("/last_name").getValueAsString()
            )
        );
      }

      //TRANSACTION
      statement.addBatch(
          "CREATE TABLE TEST.TRANSACTION_TABLE " +
              "(unique_int INT NOT NULL , t_date BIGINT, random_string VARCHAR(255))"
      );

      for (Record record : EXPECTED_TRANSACTION_RECORDS) {
        statement.addBatch(
            String.format(
                TRANSACTION_INSERT_TEMPLATE,
                "TRANSACTION_TABLE",
                record.get("/unique_int").getValueAsInteger(),
                record.get("/t_date").getValueAsLong(),
                record.get("/random_string").getValueAsString()
            )
        );
      }

      //STREAMING_TABLE
      statement.addBatch(String.format(OTHER_TABLE_CREATE_TEMPLATE, "STREAMING_TABLE"));

      //System.out.println(String.format(OTHER_TABLE_CREATE_TEMPLATE, QUOTED_TABLE_NAME));

      //QUOTE_TESTING
      statement.addBatch(String.format(OTHER_TABLE_CREATE_TEMPLATE, QUOTED_TABLE_NAME));
      statement.executeBatch();

      for (Record record : EXPECTED_QUOTE_TESTING_RECORDS) {
        statement.execute(
            String.format(
                OTHER_TABLE_INSERT_TEMPLATE,
                QUOTED_TABLE_NAME,
                record.get("/unique_int").getValueAsInteger(),
                record.get("/random_string").getValueAsString()
            )
        );
      }

    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String table : ImmutableList.of("CRICKET_STARS", "TENNIS_STARS", "TRANSACTION_TABLE",
          "STREAMING_TABLE", QUOTED_TABLE_NAME)) {
        statement.addBatch(String.format(DROP_STATEMENT_TEMPLATE, database, table));
      }
      statement.executeBatch();
    }
  }

  private List<Record> runProduceSingleBatchAndGetRecords(
      TableJdbcSource tableJdbcSource,
      Map<String, String> offsets,
      int batchSize
  ) throws Exception {
    List<Record> records = new ArrayList<>();
    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 1);
    try {
      runner.runProduce(offsets, batchSize, callback);
      records.addAll(callback.waitForAllBatchesAndReset().get(0));
      offsets.clear();
      offsets.putAll(runner.getOffsets());
    } finally {
      runner.runDestroy();
    }
    return records;
  }


  @Test
  public void testNoTableMatchesTablePatternValidationError() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("NO_TABLE%")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testSingleTableSingleBatch() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("CRICKET_STARS")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    Map<String, String> offsets = new HashMap<>();
    List<Record> records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 1000);
    Assert.assertEquals(10, records.size());
    checkRecords(EXPECTED_CRICKET_STARS_RECORDS, records);
  }

  @Test
  public void testSingleView() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("CRICKET_STARS_VIEW")
        .schema(database)
        .overrideDefaultOffsetColumns(true)
        .offsetColumns(ImmutableList.of("P_ID"))
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    Map<String, String> offsets = new HashMap<>();
    List<Record> records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 1000);
    Assert.assertEquals(10, records.size());
    checkRecords(EXPECTED_CRICKET_STARS_RECORDS, records);
  }

  @Test
  public void testSingleTableMultipleBatches() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("CRICKET_STARS")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 2);
      runner.runProduce(Collections.emptyMap(), 5, callback);

      List<List<Record>> batchRecords = callback.waitForAllBatchesAndReset();

      List<Record> records = batchRecords.get(0);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(0, 5), records);

      records = batchRecords.get(1);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(5, 10), records);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleTablesSingleBatch() throws Exception {
    TableConfigBean tableConfigBean1 =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("CRICKET_STARS")
        .schema(database)
        .build();
    TableConfigBean tableConfigBean2 =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TENNIS_STARS")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean1, tableConfigBean2))
        .build();
    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 2);
      runner.runProduce(Collections.emptyMap(), 1000, callback);

      List<List<Record>> batchRecords = callback.waitForAllBatchesAndReset();

      List<Record> records = batchRecords.get(0);
      Assert.assertEquals(10, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS, records);

      records = batchRecords.get(1);
      Assert.assertEquals(15, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS, records);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBatchStrategySwitchTables() throws Exception {
    //With a '%_STARS' regex which has to select both tables.
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%_STARS")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 5);
      runner.runProduce(Collections.emptyMap(), 5, callback);

      List<List<Record>> batchRecords = callback.waitForAllBatchesAndReset();

      List<Record> records = batchRecords.get(0);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(0, 5), records);

      records = batchRecords.get(1);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(0, 5), records);

      records = batchRecords.get(2);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(5, 10), records);

      records = batchRecords.get(3);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(5, 10), records);

      records = batchRecords.get(4);
      Assert.assertEquals(5, records.size());
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(10, 15), records);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBatchStrategyProcessAllRows() throws Exception {
    TableConfigBean tableConfigBean1 =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TENNIS_STARS")
        .schema(database)
        .build();

    TableConfigBean tableConfigBean2 =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("CRICKET_STARS")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean1, tableConfigBean2))
        .batchTableStrategy(BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE)
        .build();

    Map<String, String> offsets = new HashMap<>();
    List<Record> records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(0, 5), records);

    records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(5, 10), records);

    records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(10, 15), records);

    records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(0, 5), records);

    records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(5, 10), records);
  }

  @SuppressWarnings("unchecked")
  private void testMetricsValues(Map<String, String> offsets, String tableName) throws Exception {
    //With a '%' regex which has to select both tables.
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%_STARS")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();

    JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 1);
    Stage.Context context  = runner.getContext();
    try {
      runner.runProduce(offsets, 10, callback);
      callback.waitForAllBatchesAndReset();

      Map<String, Object> gaugeMap =
          context.getGauge(TableJdbcRunnable.TABLE_METRICS + "0").getValue();

      Assert.assertEquals(tableName, gaugeMap.get(TableJdbcRunnable.CURRENT_TABLE));
      offsets.clear();
      offsets.putAll(runner.getOffsets());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMetrics() throws Exception {
    Map<String, String> offsets = new HashMap<>();
    testMetricsValues(offsets, "TEST.CRICKET_STARS");
    testMetricsValues(offsets, "TEST.TENNIS_STARS");
  }

  @Test
  public void testOverridePartitionColumn() throws Exception {
    TableConfigBean tableConfigBean =
        new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
            .tablePattern("TRANSACTION_TABLE")
            .schema(database)
            .overrideDefaultOffsetColumns(true)
            .offsetColumns(ImmutableList.of("T_DATE"))
            .build();

    TableJdbcSource tableJdbcSource =
        new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
            .tableConfigBeans(ImmutableList.of(tableConfigBean))
            .build();

    Map<String, String> offsets = new HashMap<>();
    List<Record> records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_TRANSACTION_RECORDS.subList(0, 5), records);

    records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 5);
    Assert.assertEquals(5, records.size());
    checkRecords(EXPECTED_TRANSACTION_RECORDS.subList(5, 10), records);

    records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 10);
    Assert.assertEquals(10, records.size());
    checkRecords(EXPECTED_TRANSACTION_RECORDS.subList(10, 20), records);
  }

  private void testChangeInOffsetColumns(
      String table,
      Map<String, String> offset,
      List<String> offsetColumnsForThisRun,
      boolean shouldFail
  ) throws Exception {
    TableConfigBean tableConfigBean =
        new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
            .tablePattern(table)
            .schema(database)
            .overrideDefaultOffsetColumns(true)
            .offsetColumns(offsetColumnsForThisRun)
            .build();

    TableJdbcSource tableJdbcSource =
        new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
            .tableConfigBeans(ImmutableList.of(tableConfigBean))
            .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 1);
    runner.runProduce(offset, 5, callback);
    try {
      if (shouldFail) {
        try {
          callback.waitForAllBatchesAndReset();
          Assert.fail("Produce should fail.");
        } catch (Exception e) {
          Throwable throwable = Throwables.getRootCause(e);
          Assert.assertTrue(throwable instanceof StageException);
          StageException stageException = (StageException)throwable;
          Assert.assertEquals(JdbcErrors.JDBC_71, stageException.getErrorCode());
        }
      } else {
        List<Record> outputRecords = callback.waitForAllBatchesAndReset().get(0);
        Assert.assertEquals(5, outputRecords.size());
        offset.clear();
        offset.putAll(runner.getOffsets());
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIncreaseNumberOfOffsetColumnsInConfig() throws Exception {
    Map<String, String> offset = new HashMap<>();
    String tableName = "TRANSACTION_TABLE";
    testChangeInOffsetColumns(
        tableName,
        offset,
        ImmutableList.of("T_DATE"),
        false
    );

    //Now we added one more column to offset configuration, should fail
    testChangeInOffsetColumns(
        tableName,
        offset,
        ImmutableList.of("T_DATE", "UNIQUE_INT"),
        true
    );
  }

  @Test
  public void testDecreaseNumberOfOffsetColumnsInConfig() throws Exception {
    Map<String, String> offset = new HashMap<>();
    String tableName = "TRANSACTION_TABLE";
    testChangeInOffsetColumns(
        tableName,
        offset,
        ImmutableList.of("T_DATE", "UNIQUE_INT"),
        false
    );

    //Now we removed one column from offset configuration, should fail
    testChangeInOffsetColumns(
        tableName,
        offset,
        ImmutableList.of("T_DATE"),
        true
    );
  }

  @Test
  public void testChangeInOffsetColumnNameInConfig() throws Exception {
    Map<String, String> offset = new HashMap<>();
    String tableName = "TRANSACTION_TABLE";
    testChangeInOffsetColumns(
        tableName,
        offset,
        ImmutableList.of("UNIQUE_INT"),
        false
    );

    //Now we changed one column from offset configuration, should fail
    testChangeInOffsetColumns(
        tableName,
        offset,
        ImmutableList.of("T_DATE"),
        true
    );
  }

  @Test
  public void testStreamingInsertDuringSourceRun() throws Exception {
    TableConfigBean tableConfigBean =
        new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
            .tablePattern("STREAMING_TABLE")
            .schema(database)
            .build();
    TableJdbcSource tableJdbcSource =
        new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
            .tableConfigBeans(ImmutableList.of(tableConfigBean))
            .build();

    Map<String, String> offsets = new HashMap<>();
    for (int i = 0 ; i < 10; i++) {
      final Record record = createOtherTableRecord(i);
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                OTHER_TABLE_INSERT_TEMPLATE,
                "STREAMING_TABLE",
                record.get("/unique_int").getValueAsInteger(),
                record.get("/random_string").getValueAsString()
            )
        );
      }

      List<Record> records = runProduceSingleBatchAndGetRecords(tableJdbcSource, offsets, 1);
      Assert.assertEquals(1, records.size());
      checkRecords(ImmutableList.of(record), records);
    }
  }

  @Test
  public void testWrongInitialOffsetError() throws Exception {
    TableConfigBean tableConfigBean =
        new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
            .tablePattern("TRANSACTION_TABLE")
            .schema(database)
            .overrideDefaultOffsetColumns(true)
            .offsetColumns(ImmutableList.of("T_DATE"))
            .offsetColumnToInitialOffsetValue(
                ImmutableMap.of(
                    "T_DATE",
                    "${time:dateTimeToMilliseconds(time:extractDateFromString('abc', 'yyyy-mm-dd'))}"
                )
            )
            .build();

    TableJdbcSource tableJdbcSource =
        new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
            .tableConfigBeans(ImmutableList.of(tableConfigBean))
            .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertEquals(
        JdbcErrors.JDBC_73.getCode(),
        ((ErrorMessage)Whitebox.getInternalState(configIssues.get(0), "message")).getErrorCode()
    );
  }

  @Test
  public void testPartitionConfigs() throws Exception {
    TableConfigBean tableConfigBean =
        new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
            .tablePattern("CRICKET_STARS")
            .schema(database)
            .partitioningMode(PartitioningMode.BEST_EFFORT)
            .partitionSize("-1")
            .build();

    TableJdbcSource tableJdbcSource =
        new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
            .tableConfigBeans(ImmutableList.of(tableConfigBean))
            .build();

    validateAndAssertConfigIssue(
        tableJdbcSource,
        JdbcErrors.JDBC_101,
        TableContextUtil.GENERIC_PARTITION_SIZE_GT_ZERO_MSG
    );

    tableConfigBean.partitionSize = "abcd";

    validateAndAssertConfigIssue(
        tableJdbcSource,
        JdbcErrors.JDBC_101,
        "invalid for offset column"
    );

    tableConfigBean.partitionSize = "100";
    tableConfigBean.maxNumActivePartitions = 0;
    validateAndAssertConfigIssue(
        tableJdbcSource,
        JdbcErrors.JDBC_102,
        null
    );

    tableConfigBean.maxNumActivePartitions = 1;
    validateAndAssertConfigIssue(
        tableJdbcSource,
        JdbcErrors.JDBC_102,
        null
    );

    tableConfigBean.maxNumActivePartitions = 2;
    validateAndAssertNoConfigIssues(tableJdbcSource);
  }

  private void runSourceForInitialOffset(List<Record> expectedRecords, int batchSize, Map<String, String> lastOffsets) throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TENNIS_STARS")
        .schema(database)
        //set initial offset as id 3 (we will read from id 4)
        .offsetColumnToInitialOffsetValue(ImmutableMap.of("P_ID", "3"))
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      List<Record> actualRecords = runProduceSingleBatchAndGetRecords(tableJdbcSource, lastOffsets, batchSize);
      checkRecords(expectedRecords, actualRecords);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInitialOffset() throws Exception {
    Map<String, String> lastOffsets = new HashMap<>();
    //no last offset, read 5 records from id 4 as initial offset (read till id 9)
    runSourceForInitialOffset(EXPECTED_TENNIS_STARS_RECORDS.subList(3, 8), 5, lastOffsets);
    //Now the origin is started again with initial offset 5 but we pass the last offset, which means
    //we should read from id 9
    runSourceForInitialOffset(EXPECTED_TENNIS_STARS_RECORDS.subList(8, 15), 100, lastOffsets);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUpgradeOffsetsToV2FromLegacyFormat() throws Exception {
    Map<String, String> lastOffsets = Maps.newLinkedHashMap(
        Collections.singletonMap(
            Source.POLL_SOURCE_OFFSET_KEY, OffsetUtil.serializeOffsetMap(
                Maps.newLinkedHashMap(
                    ImmutableMap.of(
                        "TEST.CRICKET_STARS", "P_ID=5",
                        "TEST.TENNIS_STARS", "P_ID=7"
                    )
                )
            )
        )
    );
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%_STARS")
        .schema(database)
        .partitioningMode(PartitioningMode.DISABLED)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    tableJdbcSource = Mockito.spy(tableJdbcSource);

    PushSourceRunner runner = Mockito.spy(
        new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
            .addOutputLane("a")
            .setOnRecordError(OnRecordError.TO_ERROR)
            .build()
    );

    //Check offset upgrade
    Mockito.doAnswer(invocationOnMock -> {
      Map<String, String> lastOffsetStringMap = (Map<String, String>) invocationOnMock.getArguments()[0];

      Assert.assertTrue(lastOffsetStringMap.containsKey(Source.POLL_SOURCE_OFFSET_KEY));

      Map<String, String> oldOffsetMap =
          OffsetUtil.deserializeOffsetMap(lastOffsetStringMap.get(Source.POLL_SOURCE_OFFSET_KEY));


      TableJdbcSource source = (TableJdbcSource) invocationOnMock.getMock();

      Object returnVal = invocationOnMock.callRealMethod();

      //Check the local offsets variable from the TableJdbcSource
      Map<String, String> upgradedOffset = (Map<String, String>) Whitebox.getInternalState(source, "offsets");

      MultithreadedTableProvider tableProvider = (MultithreadedTableProvider) Whitebox.getInternalState(
          source,
          "tableOrderProvider"
      );
      //Should not contain poll source offset key
      Assert.assertFalse(upgradedOffset.containsKey(Source.POLL_SOURCE_OFFSET_KEY));

      //Contain all other table values
      for (Map.Entry<String, String> oldOffsetEntry : oldOffsetMap.entrySet()) {
        final String tableName = oldOffsetEntry.getKey();

        TableRuntimeContext partition = getSinglePartitionForTable(tableProvider, tableName);
        Assert.assertTrue(upgradedOffset.containsKey(partition.getOffsetKey()));
        Assert.assertEquals(oldOffsetEntry.getValue(), upgradedOffset.get(partition.getOffsetKey()));
      }
      //Call the real method, thus upgrading
      return returnVal;
    }).when(tableJdbcSource).handleLastOffset(Mockito.anyMapOf(String.class, String.class));

    runner.runInit();

    JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 2);

    try {
      runner.runProduce(lastOffsets, 10, callback);
      List<List<Record>> batches = callback.waitForAllBatchesAndReset();
      Assert.assertEquals(2, batches.size());

      checkRecords(EXPECTED_CRICKET_STARS_RECORDS.subList(5, EXPECTED_CRICKET_STARS_RECORDS.size()), batches.get(0));
      checkRecords(EXPECTED_TENNIS_STARS_RECORDS.subList(7, EXPECTED_TENNIS_STARS_RECORDS.size()), batches.get(1));

      Map<String, String> runnerOffsets = runner.getOffsets();

      //Check the offsets after upgraded offsets are used by the runner
      Assert.assertEquals(3, runnerOffsets.size());
      Assert.assertFalse(runnerOffsets.containsKey(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertTrue(runnerOffsets.containsKey(TableJdbcSource.OFFSET_VERSION));

      Assert.assertEquals(TableJdbcSource.OFFSET_VERSION_2, runnerOffsets.get(TableJdbcSource.OFFSET_VERSION));

      MultithreadedTableProvider tableProvider = (MultithreadedTableProvider) Whitebox.getInternalState(
          tableJdbcSource,
          "tableOrderProvider"
      );

      TableRuntimeContext cricketStarsPartition = getSinglePartitionForTable(tableProvider, "TEST.CRICKET_STARS");
      TableRuntimeContext tennisStarsPartition = getSinglePartitionForTable(tableProvider, "TEST.TENNIS_STARS");

      Assert.assertTrue(runnerOffsets.containsKey(cricketStarsPartition.getOffsetKey()));
      Assert.assertTrue(runnerOffsets.containsKey(tennisStarsPartition.getOffsetKey()));

      Assert.assertEquals("P_ID=10", runnerOffsets.get(cricketStarsPartition.getOffsetKey()));
      Assert.assertEquals("P_ID=15", runnerOffsets.get(tennisStarsPartition.getOffsetKey()));
    } finally {
      runner.runDestroy();
    }
  }

  private static TableRuntimeContext getSinglePartitionForTable(
      MultithreadedTableProvider tableProvider,
      String tableName
  ) {
    Assert.assertTrue(tableProvider.getTableContextMap().containsKey(tableName));
    final TableContext tableContext = tableProvider.getTableContextMap().get(tableName);
    Assert.assertTrue(tableProvider.getActiveRuntimeContexts().containsKey(tableContext));
    Assert.assertTrue(tableProvider.getActiveRuntimeContexts().get(tableContext).size() == 1);
    return tableProvider.getActiveRuntimeContexts().get(tableContext).first();
  }

  @Test
  public void testQuoting() throws Exception  {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern(QUOTED_TABLE_NAME_WITHOUT_QUOTES)
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .quoteChar(QuoteChar.BACKTICK)
        .build();
    List<Record> records = runProduceSingleBatchAndGetRecords(tableJdbcSource, new HashMap<>(), 20);
    checkRecords(EXPECTED_QUOTE_TESTING_RECORDS, records);
  }
}
