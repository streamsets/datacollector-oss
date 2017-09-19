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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.event.CommonEvents;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultiThreadedIT extends BaseTableJdbcSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedIT.class);
  private static final String TABLE_NAME_PREFIX = "TAB";
  private static final String COLUMN_NAME_PREFIX = "col";
  private static final String OFFSET_FIELD_NAME = "off";
  private static final String OFFSET_FIELD_RAW_NAME = "off_raw";
  private static final int NON_INCREMENTAL_LOAD_TEST_TABLE_NUMBER = 1;
  private static final int NUMBER_OF_TABLES = 10;
  private static final int NUMBER_OF_COLUMNS_PER_TABLE = 1;
  private static final int NUMBER_OF_THREADS = 4;
  private static final int MAX_ROWS_PER_TABLE = 100000;
  private static final List<Field.Type> OTHER_FIELD_TYPES =
      Arrays.stream(Field.Type.values())
          .filter(
              fieldType ->
                  !fieldType.isOneOf(
                      Field.Type.LIST_MAP,
                      Field.Type.MAP,
                      Field.Type.LIST,
                      Field.Type.FILE_REF,
                      Field.Type.BYTE_ARRAY,
                      Field.Type.BYTE,
                      Field.Type.CHAR
                  )
          )
          .collect(Collectors.toList());
  private static final Map<String, List<Record>> EXPECTED_TABLES_TO_RECORDS = new LinkedHashMap<>();

  @Rule
  public Timeout globalTimeout = Timeout.seconds(300); // 5 minutes

  private static void populateColumns(
      Map<String, Field.Type> columnToFieldType,
      Map<String, String> offsetColumns,
      Map<String, String> otherColumns
  ) {
    columnToFieldType.put(OFFSET_FIELD_NAME, Field.Type.INTEGER);

    offsetColumns.put(
        OFFSET_FIELD_NAME,
        FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(columnToFieldType.get(OFFSET_FIELD_NAME))
    );

    IntStream.range(0, NUMBER_OF_COLUMNS_PER_TABLE).forEach(columnNumber -> {
      String columnName = COLUMN_NAME_PREFIX + columnNumber;
      columnToFieldType.put(
          columnName,
          OTHER_FIELD_TYPES.get(RANDOM.nextInt(OTHER_FIELD_TYPES.size()))
      );
      otherColumns.put(columnName, FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(columnToFieldType.get(columnName)));
    });
  }

  private static void createRecordsAndExecuteInsertStatements(
      String tableName,
      Statement st,
      Map<String, Field.Type> columnToFieldType,
      boolean multipleRecordsWithSameOffset
  ) throws Exception {
    int numberOfRecordsInTable = RANDOM.nextInt(MAX_ROWS_PER_TABLE - 1) + 1;
    IntStream.range(0, numberOfRecordsInTable).forEach(recordNum -> {
      Record record = RecordCreator.create();
      LinkedHashMap<String, Field> rootField = new LinkedHashMap<>();

      final int recordNumDivisor = multipleRecordsWithSameOffset ? 100 : 1;
      final int calculatedRecordNum = recordNum / recordNumDivisor;

      rootField.put(OFFSET_FIELD_NAME, Field.create(calculatedRecordNum));


      columnToFieldType.entrySet().stream()
          .filter(columnToFieldTypeEntry -> !columnToFieldTypeEntry.getKey().equals(OFFSET_FIELD_NAME))
          .forEach(columnToFieldTypeEntry  -> {
            String fieldName = columnToFieldTypeEntry.getKey();
            Field.Type fieldType = columnToFieldTypeEntry.getValue();
            rootField.put(fieldName, Field.create(fieldType, generateRandomData(fieldType)));
          });

      if (multipleRecordsWithSameOffset) {
        rootField.put(OFFSET_FIELD_RAW_NAME, Field.create(recordNum));
      }
      record.set(Field.createListMap(rootField));
      List<Record> records = EXPECTED_TABLES_TO_RECORDS.computeIfAbsent(tableName, t -> new ArrayList<>());
      records.add(record);

      try {
        st.addBatch(getInsertStatement(database, tableName, rootField.values()));
      } catch (Exception e) {
        LOG.error("Error Happened", e);
        Throwables.propagate(e);
      }
    });
    st.executeBatch();
  }

  @BeforeClass
  public static void setupTables() throws Exception {
    IntStream.range(0, NUMBER_OF_TABLES).forEach(tableNumber -> {
      try (Statement st = connection.createStatement()){
        final boolean multipleRecordsWithSameOffsetTable = tableNumber == 0;
        final boolean nonIncrementalTestTable = tableNumber == NON_INCREMENTAL_LOAD_TEST_TABLE_NUMBER;
        String tableName = TABLE_NAME_PREFIX + tableNumber;
        Map<String, Field.Type> columnToFieldType = new LinkedHashMap<>();
        Map<String, String> offsetColumns = new LinkedHashMap<>();
        Map<String, String> otherColumns = new LinkedHashMap<>();
        populateColumns(columnToFieldType, offsetColumns, otherColumns);
        if (multipleRecordsWithSameOffsetTable) {
          columnToFieldType.put(OFFSET_FIELD_RAW_NAME, Field.Type.INTEGER);
          otherColumns.put(OFFSET_FIELD_RAW_NAME, FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(Field.Type.INTEGER));
        }
        st.execute(
            getCreateStatement(
                database,
                tableName,
                offsetColumns,
                otherColumns,
                !multipleRecordsWithSameOffsetTable && !nonIncrementalTestTable
            )
        );
        createRecordsAndExecuteInsertStatements(tableName, st, columnToFieldType, multipleRecordsWithSameOffsetTable);
      } catch (Exception e) {
        LOG.error("Error Happened", e);
        Throwables.propagate(e);
      }
    });
  }

  @AfterClass
  public static void deleteTables() throws Exception {
    try (Statement st = connection.createStatement()) {
      EXPECTED_TABLES_TO_RECORDS.keySet().forEach(table -> {
            try {
              st.addBatch(String.format(DROP_STATEMENT_TEMPLATE, database, table));
            } catch (SQLException e) {
              LOG.error("Error Happened", e);
              Throwables.propagate(e);
            }
          }
      );
    }
  }

  private static class MultiThreadedJdbcTestCallback implements PushSourceRunner.Callback {
    private final PushSourceRunner pushSourceRunner;
    private final Map<String, List<Record>> tableToRecords;
    private final Set<String> tablesYetToBeCompletelyRead;

    private MultiThreadedJdbcTestCallback(PushSourceRunner pushSourceRunner, Set<String> tables) {
      this.pushSourceRunner = pushSourceRunner;
      this.tableToRecords = new ConcurrentHashMap<>();
      this.tablesYetToBeCompletelyRead = new HashSet<>(tables);
    }

    private Map<String, List<Record>> waitForAllBatchesAndReset() {
      try {
        pushSourceRunner.waitOnProduce();
      } catch (Exception e) {
        Throwables.propagate(e);
        Assert.fail(e.getMessage());
      }
      return ImmutableMap.copyOf(tableToRecords);
    }

    @Override
    public synchronized void processBatch(StageRunner.Output output) {
      List<Record> records = output.getRecords().get("a");
      if (!records.isEmpty()) {
        Record record = records.get(0);
        String tableName = record.getHeader().getAttribute("jdbc.tables");
        List<Record> recordList =
            tableToRecords.computeIfAbsent(tableName, table -> Collections.synchronizedList(new ArrayList<>()));
        recordList.addAll(records);
        List<Record> expectedRecords = EXPECTED_TABLES_TO_RECORDS.get(tableName);
        if (expectedRecords.size() <= recordList.size()) {
          tablesYetToBeCompletelyRead.remove(tableName);
        }
      }
      List<Record> eventRecords = pushSourceRunner.getEventRecords();
      if (tablesYetToBeCompletelyRead.isEmpty() && !eventRecords.isEmpty()) {
        pushSourceRunner.setStop();
      }
    }
  }

  public void testMultiThreadedRead(TableJdbcSource tableJdbcSource, String... tables) throws Exception {
    testMultiThreadedRead(tableJdbcSource, new HashSet<>(Arrays.asList(tables)));
  }

  public void testMultiThreadedRead(TableJdbcSource tableJdbcSource) throws Exception {
    testMultiThreadedRead(tableJdbcSource, EXPECTED_TABLES_TO_RECORDS.keySet());
  }

  public void testMultiThreadedRead(TableJdbcSource tableJdbcSource, Set<String> tables) throws Exception {
    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    MultiThreadedJdbcTestCallback multiThreadedJdbcTestCallback = new MultiThreadedJdbcTestCallback(runner, tables);

    try {
      runner.runProduce(Collections.emptyMap(), 20, multiThreadedJdbcTestCallback);
      Map<String, List<Record>> actualTableToRecords = multiThreadedJdbcTestCallback.waitForAllBatchesAndReset();
      Assert.assertEquals(tables.size(), actualTableToRecords.size());

      EXPECTED_TABLES_TO_RECORDS.keySet().stream().filter(table -> tables.contains(table)).forEach((tableName) -> {
        final List<Record> expectedRecords = EXPECTED_TABLES_TO_RECORDS.get(tableName);
        List<Record> actualRecords = actualTableToRecords.get(tableName);
        checkRecords(tableName, expectedRecords, actualRecords, new Comparator<Record>() {
          @Override
          public int compare(Record o1, Record o2) {
            final Field off1 = o1.get("/OFF");
            final Field off2 = o2.get("/OFF");
            if (off1 == null) {
              if (off2 == null) {
                return 0;
              } else {
                return 1;
              }
            } else if (off2 == null) {
              return -1;
            }
            return off1.getValueAsInteger() - off2.getValueAsInteger();
          }
        });
      });

      Assert.assertEquals(1, runner.getEventRecords().size());
      //Assert.assertTrue(1<=runner.getEventRecords().size());
      Record eventRecord = runner.getEventRecords().get(0);
      String eventType = eventRecord.getHeader().getAttribute("sdc.event.type");
      Assert.assertNotNull(eventType);
      Assert.assertTrue(CommonEvents.NO_MORE_DATA_TAG.equals(eventType));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSwitchTables() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .maxNumActivePartitions(6)
        .partitioningMode(PartitioningMode.BEST_EFFORT)
        .partitionSize("1000")
        .schema(database)
        .offsetColumns(Collections.singletonList(OFFSET_FIELD_NAME.toUpperCase()))
        .overrideDefaultOffsetColumns(true)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .batchTableStrategy(BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE)
        // 4 threads
        .numberOfThreads(NUMBER_OF_THREADS)
        .build();
    testMultiThreadedRead(tableJdbcSource);
  }

  @Test
  public void testProcessAllRows() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(database)
        .partitionSize("1000")
        .partitioningMode(PartitioningMode.BEST_EFFORT)
        .offsetColumns(Collections.singletonList(OFFSET_FIELD_NAME.toUpperCase()))
        .overrideDefaultOffsetColumns(true)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .batchTableStrategy(BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE)
        // 4 threads
        .numberOfThreads(NUMBER_OF_THREADS)
        .build();

    testMultiThreadedRead(tableJdbcSource);
  }

  @Test
  public void testSwitchTablesWithNumberOfBatches() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(database)
        .partitionSize("1000")
        .partitioningMode(PartitioningMode.BEST_EFFORT)
        .offsetColumns(Collections.singletonList(OFFSET_FIELD_NAME.toUpperCase()))
        .overrideDefaultOffsetColumns(true)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        // 4 threads
        .numberOfThreads(NUMBER_OF_THREADS)
        .batchTableStrategy(BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE)
        .numberOfBatchesFromResultset(2)
        .build();

    testMultiThreadedRead(tableJdbcSource);
  }

  @Test
  public void testNonIncrementalLoad() throws Exception {

    final String nonIncrementalTable = TABLE_NAME_PREFIX + NON_INCREMENTAL_LOAD_TEST_TABLE_NUMBER;
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern(nonIncrementalTable)
        .schema(database)
        .partitionSize("1000")
        .partitioningMode(PartitioningMode.BEST_EFFORT)
        .enableNonIncremental(true)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .batchTableStrategy(BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE)
        // 4 threads
        .numberOfThreads(NUMBER_OF_THREADS)
        .build();

    testMultiThreadedRead(tableJdbcSource, nonIncrementalTable);
  }

  @Test
  @Ignore("Figure out how to handle max tables (partitions) per thread map now: SDC-6768")
  public void testNumThreadsMoreThanNumTables() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(database)
        .offsetColumns(Collections.singletonList(OFFSET_FIELD_NAME.toUpperCase()))
        .overrideDefaultOffsetColumns(true)
        .build();
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        // 20 threads in config but there are only 10 tables
        .numberOfThreads(NUMBER_OF_TABLES + 10)
        .batchTableStrategy(BatchTableStrategy.SWITCH_TABLES)
        .build();
    tableJdbcSource = PowerMockito.spy(tableJdbcSource);
    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      int numberOfThreads = Whitebox.getInternalState(tableJdbcSource, "numberOfThreads");
      Assert.assertEquals(NUMBER_OF_TABLES, numberOfThreads);
    } finally {
      runner.runDestroy();
    }
  }
}
