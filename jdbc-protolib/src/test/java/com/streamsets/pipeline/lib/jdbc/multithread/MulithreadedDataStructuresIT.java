/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.jdbc.table.BaseTableJdbcSourceIT;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBeanImpl;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcDSource;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSourceTestBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class MulithreadedDataStructuresIT extends BaseTableJdbcSourceIT {
  private static final String LANE_NAME = "a";
  private static final String TABLE_NAME_PREFIX = "TAB";
  private static final String COLUMN_NAME_PREFIX = "col";
  private static final String OFFSET_FIELD_NAME = "ffo";
  private static final int NUMBER_OF_COLUMNS_PER_TABLE = 1;
  private static final int NUMBER_OF_TABLES = 4;
  private static final int NUMBER_OF_THREADS = 2;
  private static final int MAX_BATCH_SIZE = 2;
  private static final int PARTITION_SIZE = 6;

  private static final List<Integer> ROWS_PER_TABLE = Arrays.asList(12, 24, 6, 12);
  private static final Map<String, List<Record>> EXPECTED_TABLES_TO_RECORDS = new LinkedHashMap<>();

  @Rule
  public Timeout globalTimeout = Timeout.seconds(300);

  @Parameterized.Parameters
  public static List<Object[]> parameters() {
    return Arrays.asList(
      new Object[]{PartitioningMode.REQUIRED, BatchTableStrategy.SWITCH_TABLES},
      new Object[]{PartitioningMode.REQUIRED, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE},
      new Object[]{PartitioningMode.DISABLED, BatchTableStrategy.SWITCH_TABLES},
      new Object[]{PartitioningMode.DISABLED, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE}
    );
  }

  private final BatchTableStrategy batchTableStrategy;
  private final PartitioningMode partitioningMode;

  public MulithreadedDataStructuresIT(final PartitioningMode partitioningMode, final BatchTableStrategy batchTableStrategy) {
    this.partitioningMode = partitioningMode;
    this.batchTableStrategy = batchTableStrategy;
  }

  private static void populateColumns(
      final Map<String, Field.Type> columnToFieldType,
      final Map<String, String> offsetColumns,
      final Map<String, String> otherColumns
  ) {
    columnToFieldType.put(OFFSET_FIELD_NAME, Field.Type.INTEGER);

    offsetColumns.put(
        OFFSET_FIELD_NAME,
        FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(columnToFieldType.get(OFFSET_FIELD_NAME))
    );

    for (int columnNumber = 0; columnNumber < NUMBER_OF_COLUMNS_PER_TABLE; columnNumber += 1) {
      String columnName = COLUMN_NAME_PREFIX + columnNumber;
      columnToFieldType.put(columnName, Field.Type.STRING);
      otherColumns.put(columnName, FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(columnToFieldType.get(columnName)));
    }
  }

  private static void createRecordsAndExecuteInsertStatements(
      final String tableName,
      final int numberOfRecordsInTable,
      final Statement st,
      final Map<String, Field.Type> columnToFieldType
  ) throws Exception {
    for (int recordNum = 0; recordNum < numberOfRecordsInTable; recordNum += 1) {
      Record record = RecordCreator.create();
      LinkedHashMap<String, Field> rootField = new LinkedHashMap<>();

      rootField.put(OFFSET_FIELD_NAME, Field.create(recordNum));

      columnToFieldType.entrySet().stream()
          .filter(columnToFieldTypeEntry -> !columnToFieldTypeEntry.getKey().equals(OFFSET_FIELD_NAME))
          .forEach(columnToFieldTypeEntry  -> {
            String fieldName = columnToFieldTypeEntry.getKey();
            Field.Type fieldType = columnToFieldTypeEntry.getValue();
            rootField.put(fieldName, Field.create(fieldType, generateRandomData(fieldType)));
          });

      record.set(Field.createListMap(rootField));

      List<Record> records = EXPECTED_TABLES_TO_RECORDS.computeIfAbsent(tableName, t -> new ArrayList<>());
      records.add(record);

      try {
        st.addBatch(getInsertStatement(database, tableName, rootField.values()));
      } catch (final Exception ex) {
        Throwables.propagate(ex);
      }
    }

    st.executeBatch();
  }

  @BeforeClass
  public static void setupTables() {
    for(int tableNumber = 0; tableNumber < NUMBER_OF_TABLES; tableNumber += 1) {
      try (Statement st = connection.createStatement()){
        String tableName = TABLE_NAME_PREFIX + tableNumber;
        Map<String, Field.Type> columnToFieldType = new LinkedHashMap<>();
        Map<String, String> offsetColumns = new LinkedHashMap<>();
        Map<String, String> otherColumns = new LinkedHashMap<>();

        populateColumns(columnToFieldType, offsetColumns, otherColumns);

        st.execute(getCreateStatement(
            database,
            tableName,
            offsetColumns,
            otherColumns,
            true
        ));

        createRecordsAndExecuteInsertStatements(tableName, ROWS_PER_TABLE.get(tableNumber), st, columnToFieldType);
      } catch (final Exception ex) {
        Throwables.propagate(ex);
      }
    }
  }

  @AfterClass
  public static void deleteTables() throws Exception {
    try (Statement st = connection.createStatement()) {
      for (final String table : EXPECTED_TABLES_TO_RECORDS.keySet()) {
        try {
          st.addBatch(String.format(DROP_STATEMENT_TEMPLATE, database, table));
        } catch (final SQLException ex) {
          Throwables.propagate(ex);
        }
      }
    }
  }

  @Test
  public void testOwnedTablesLocked() throws Throwable {
    RecordProcessor[] callback = new RecordProcessor[]{null};
    Throwable[] exception = new Throwable[]{null};
    Map<Thread, List<String>> threadWorkedPartitions = new HashMap<>();
    Map<Thread, List<String>> threadPartitions = new HashMap<>();

    TableConfigBeanImpl tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .partitioningMode(partitioningMode)
        .partitionSize("" + PARTITION_SIZE)
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .batchTableStrategy(batchTableStrategy)
        .numberOfThreads(NUMBER_OF_THREADS)
        .postProcessBatchCallback(tableProvider -> {
          List<TableRuntimeContext> sharedAvailableTables;
          List<TableRuntimeContext> ownedPartitions;

          synchronized(tableProvider.getActiveRuntimeContexts()) {
            sharedAvailableTables = new ArrayList<>(tableProvider.getSharedAvailableTablesList());
            ownedPartitions = new ArrayList<>(tableProvider.getOwnedTablesQueue());
          }

          // There will be latency and seems there is no big difference,
          // if we copy data structures and execute checks later
          // or if we walk through the lists right away,
          // copying also requires to walk through lists.
          // Besides, it's hard  to say, if bugs manifest themselves when there is or there is no latency.
          synchronized (MulithreadedDataStructuresIT.this) {
            try {
              List<String> newCurrentOwnedPartitions = ownedPartitions.stream().map(TableRuntimeContext::getDescription).collect(Collectors.toList());

              threadPartitions.put(Thread.currentThread(), newCurrentOwnedPartitions);

              // No interactions
              for (final String partition : newCurrentOwnedPartitions) {
                for (final Map.Entry<Thread, List<String>> entry : threadPartitions.entrySet()) {
                  if (entry.getKey() != Thread.currentThread()) {
                    // This thread owns certain partitions
                    // We want to make sure they are not owned by any other thread.
                    Assert.assertTrue(entry.getValue().stream().noneMatch(p -> p.contains(partition)));
                  }
                }
              }

              List<String> workedPartitions = threadWorkedPartitions.computeIfAbsent(Thread.currentThread(), th -> new ArrayList<>());
              workedPartitions.addAll(newCurrentOwnedPartitions);

              // We do not own more than 1 partition of the same table.
              // If we own partitions of the same table the size of the set containing table names only
              // will be less than the original list size.
              Assert.assertEquals(
                  ownedPartitions.stream()
                    .map(ot -> ot.getDescription().replaceAll(".*tableName=([^;]+);;;.*", "$1"))
                    .collect(Collectors.toSet())
                    .size(),
                  ownedPartitions.size()
              );

              // If a table is owned it cannot be in the shared table list.
              for (final TableRuntimeContext ot : ownedPartitions) {
                Assert.assertFalse(sharedAvailableTables.contains(ot));
              }

              Set<String> partitions = callback[0].threadPartitions.get(Thread.currentThread());
              if (partitions != null) {
                // The partition stays in the owned list... (see below for the continuation)
                for (final String partitionName : partitions) {
                  // So we know that this thread created records of a certain partition
                  Integer size = callback[0].partitionRecordsToRead.get(partitionName);
                  if (size != null && size > 0) { // And we haven't yet processed all partition records
                    // We want to make sure the partition is in the owned list
                    Assert.assertTrue(newCurrentOwnedPartitions.contains(partitionName));
                  }
                }
              }
            } catch (final Throwable th) {
              exception[0] = th;
            }
          }
        })
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane(LANE_NAME)
        .build();

    callback[0] = new RecordProcessor(runner);

    runner.runInit();

    try {
      runner.runProduce(Collections.emptyMap(), MAX_BATCH_SIZE, callback[0]);
      runner.waitOnProduce();
    } finally {
      runner.runDestroy();
    }

    if (exception[0] != null) {
      throw exception[0];
    }

    boolean noJumpBack = true; //Jump back: 1 1 1 1 2 2 2 2 1 1 1 1 - No jump back: 1 1 1 1 or 1 1 2 2 or 1 1 2 2 3 3
    boolean noTableSwitch = true; // Switch: 1 1 1 1 2 2 2 2 - No switch: 1 1 1 1 1 1 1 1 1
    for (final Map.Entry<Thread, List<String>> entry : threadWorkedPartitions.entrySet()) {
      boolean wasJumpBack = false;
      boolean wasTableSwicth = false;
      String prevPartition = null;
      Map<String, Boolean> jumpBacks = new HashMap<>();
      for (final String partition : entry.getValue()) {
        boolean tableSwitch = !partition.equals(prevPartition);
        Boolean jumpBack = jumpBacks.get(partition);
        if (jumpBack == null) {
          jumpBacks.put(partition, false);
        } else {
          if (tableSwitch) {
            jumpBacks.put(partition, true);
            wasJumpBack = true;
          }
        }

        wasTableSwicth = wasTableSwicth || tableSwitch;
        prevPartition = partition;
      }
      noJumpBack = noJumpBack && !wasJumpBack;
      noTableSwitch = noTableSwitch && !wasTableSwicth;
    }

    // (see the beginning above) ... even if we switch to another partition
    // We proved that a partition stays in the owned list (see above)
    // We also proved there were no intersections, thus a partition was owned by one thread only.
    // Now we prove that there are switches between partition.
    // It means that a partition stays in the owned list even if there are switches  between partitions.
    Assert.assertFalse(noTableSwitch);

    if (batchTableStrategy == BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE) {
      // We processed all records from one partition before we jump to another partition (there were no jump backs)
      Assert.assertTrue(noJumpBack);
    } else {
      // With 2 threads and 4 tables, partition size = 6 and batch size = 2
      // we expect that at least one thread worked on the same partition at least twice.
      Assert.assertFalse(noJumpBack);
    }
  }

  private final class RecordProcessor implements PushSourceRunner.Callback {
    private final AtomicBoolean noMoreDataEvent = new AtomicBoolean(false);
    private final Set<String> tablesToRead = new HashSet<>(EXPECTED_TABLES_TO_RECORDS.keySet());
    private final Map<Thread, Set<String>> threadPartitions = new HashMap<>();
    private final Map<String, Integer> partitionRecordsToRead = new HashMap<>();
    private final Map<String, List<Record>> tableRecords = new HashMap<>();

    private final PushSourceRunner runner;

    private RecordProcessor(final PushSourceRunner runner) {
      this.runner = runner;
    }

    @Override
    public void processBatch(final StageRunner.Output output) throws StageException {
      synchronized (MulithreadedDataStructuresIT.this) {
        List<Record> records = output.getRecords().get(LANE_NAME);

        if (!records.isEmpty()) {
          Record record = records.get(0);

          String tableName = record.getHeader().getAttribute("jdbc.tables");
          String partitionName = record.getHeader().getAttribute("jdbc.partition");

          Set<String> thisThreadPartitions = threadPartitions.computeIfAbsent(Thread.currentThread(), th -> new HashSet<>());
          thisThreadPartitions.add(partitionName);

          Integer old = partitionRecordsToRead.computeIfAbsent(partitionName, key -> 6);
          partitionRecordsToRead.put(partitionName, old - records.size());

          List<Record> recordList = tableRecords.computeIfAbsent(
              tableName,
              table -> Collections.synchronizedList(new ArrayList<>())
          );
          recordList.addAll(records);
          List<Record> expectedRecords = EXPECTED_TABLES_TO_RECORDS.get(tableName);
          if (expectedRecords.size() <= recordList.size()) {
            tablesToRead.remove(tableName);
          }
        }

        List<EventRecord> eventRecords = new LinkedList<>(runner.getEventRecords());
        if (!noMoreDataEvent.get()) {
          for (final EventRecord eventRecord : eventRecords) {
            if (eventRecord != null) {
              if (NoMoreDataEvent.NO_MORE_DATA_TAG.equals(eventRecord.getEventType())) {
                noMoreDataEvent.set(true);
                break;
              }
            }
          }
        }

        if (tablesToRead.isEmpty() && noMoreDataEvent.get()) {
          runner.setStop();
        }
      }
    }
  }
}
