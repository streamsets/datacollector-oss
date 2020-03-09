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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.collect.SortedSetMultimap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.vividsolutions.jts.util.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestMultithreadedTableProvider extends BaseMultithreadedTableProviderTest {

  @Test
  public void basicPartitioning() throws InterruptedException {

    int batchSize = 10;
    String schema = "db";
    String table1Name = "table1";
    String offsetCol = "col";
    final String partitionSize = "100";
    int maxActivePartitions = 3;
    int threadNumber = 0;
    int numThreads = 1;

    TableContext table1 = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, true);

    MultithreadedTableProvider provider = createTableProvider(numThreads, table1, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);

    SortedSetMultimap<TableContext, TableRuntimeContext> partitions = provider.getActiveRuntimeContexts();
    assertThat(partitions.size(), equalTo(1));
    assertTrue(partitions.containsKey(table1));

    TableRuntimeContext part1 = partitions.get(table1).first();

    validatePartition(part1, 1, table1, false, false, false, false, offsetCol, partitionSize);

    // simulate a row being processed for the first partition
    part1.recordColumnOffset(offsetCol, "0");
    part1.recordColumnOffset(offsetCol, "1");

    assertThat(part1.isAnyOffsetsRecorded(), equalTo(true));
    part1.setResultSetProduced(true);

    validatePartition(part1, 1, table1, true, true, true, false, offsetCol, partitionSize);

    provider.reportDataOrNoMoreData(part1, batchSize, batchSize, false);

    TableRuntimeContext part1Again = provider.nextTable(threadNumber);

    validatePartition(part1Again, 1, table1, true, true, true, false, offsetCol, partitionSize);

    provider.releaseOwnedTable(part1Again, threadNumber);
    TableRuntimeContext part2 = provider.nextTable(threadNumber);
    validatePartition(part2, 2, table1, false, false, true, false, offsetCol, partitionSize);
    provider.releaseOwnedTable(part2, threadNumber);
    TableRuntimeContext part3 = provider.nextTable(threadNumber);
    validatePartition(part3, 3, table1, false, false, true, false, offsetCol, partitionSize);

    // at this point, no partitions should allow a next partition
    assertThat(provider.isNewPartitionAllowed(part1), equalTo(false));
    assertThat(provider.isNewPartitionAllowed(part2), equalTo(false));
    assertThat(provider.isNewPartitionAllowed(part3), equalTo(false));

    // marking the first finished
    provider.reportDataOrNoMoreData(part1, batchSize - 1, batchSize, true);

    // simulate record for 2nd partition
    part2.recordColumnOffset(offsetCol, "101");

    // this should now remove the 1st from active contexts...
    provider.reportDataOrNoMoreData(part2, batchSize, batchSize, false);
    assertThat(provider.getActiveRuntimeContexts().size(), equalTo(2));

    //... thus freeing up the possibility for a new one
    assertThat(provider.isNewPartitionAllowed(part3), equalTo(true));
    provider.releaseOwnedTable(part3, threadNumber);

    part2 = provider.nextTable(threadNumber);
    provider.releaseOwnedTable(part2, threadNumber);

    // this actually creates part4
    part3 = provider.nextTable(threadNumber);
    provider.releaseOwnedTable(part3, threadNumber);

    // but part2 is first on the queue again; release it
    part2 = provider.nextTable(threadNumber);
    provider.releaseOwnedTable(part2, threadNumber);

    TableRuntimeContext part4 = provider.nextTable(threadNumber);
    validatePartition(part4, 4, table1, false, false, true, false, offsetCol, partitionSize);

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));
    provider.releaseOwnedTable(part4, threadNumber);

    part3 = provider.nextTable(threadNumber);
    part3.setResultSetProduced(true);
    provider.reportDataOrNoMoreData(part3, 0, batchSize, true);
    validatePartition(part3, 3, table1, false, true, true, true, offsetCol, partitionSize);
    provider.releaseOwnedTable(part3, threadNumber);

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));

    part2 = provider.nextTable(threadNumber);
    part2.setResultSetProduced(true);
    provider.reportDataOrNoMoreData(part2, 0, batchSize, true);
    validatePartition(part2, 2, table1, true, true, true, true, offsetCol, partitionSize);
    provider.releaseOwnedTable(part2, threadNumber);

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));
    assertThat(provider.getTablesWithNoMoreData(), hasSize(0));

    part4 = provider.nextTable(threadNumber);
    part4.recordColumnOffset(offsetCol, "301");
    part4.setResultSetProduced(true);
    provider.reportDataOrNoMoreData(part4, 1, batchSize, true);
    validatePartition(part4, 4, table1, true, true, true, true, offsetCol, partitionSize);
    provider.releaseOwnedTable(part4, threadNumber);

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));

    // suppose that "301" (in partition 4) was the last actual record... need to generate next 3 partitions in order
    // for provider to consider the table out of data since that's the max # active partitions
    // five iterations, because the first one gives part4 again
    // then we cycle through part4, part5, part6
    for (int i=0; i<3; i++) {
      TableRuntimeContext nextPart = provider.nextTable(threadNumber);
      nextPart.setResultSetProduced(true);
      provider.reportDataOrNoMoreData(nextPart, 0, batchSize, true);
      provider.releaseOwnedTable(nextPart, threadNumber);
    }
    // after part6 is marked no more data (last loop iteration above), we finally consider the table to be out of data,
    // because the last partition seen with data was part4, so it has now been more than the max number of active
    // partitions (3) since we have seen any data

//    part4 = provider.nextTable(threadNumber);
//    validatePartition(part4, 4, table1, false, true, true, true, offsetCol, partitionSize);


    assertThat(provider.getTablesWithNoMoreData(), hasSize(1));
    assertThat(provider.getTablesWithNoMoreData().iterator().next(), equalTo(table1));
    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(true));

    // mark partition 3, then 2 as finished
  }

  @Test
  public void nonIncremental() throws InterruptedException {

    final String schema = "db";
    final String table1Name = "table1";
    final int maxActivePartitions = 100;
    final int threadNumber = 1;

    TableContext table1 = createTableContext(
        schema,
        table1Name,
        null,
        TableConfigBean.DEFAULT_PARTITION_SIZE,
        null,
        maxActivePartitions,
        false,
        true
    );

    MultithreadedTableProvider provider = createTableProvider(1, table1, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));

    TableRuntimeContext part1 = provider.nextTable(threadNumber);

    assertThat(provider.isNewPartitionAllowed(part1), equalTo(false));

    final SortedSetMultimap<TableContext, TableRuntimeContext> activePartitions = provider.getActiveRuntimeContexts();
    assertThat(activePartitions.size(), equalTo(1));

    provider.reportDataOrNoMoreData(part1, 1000, 1000, false);

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));

    provider.reportDataOrNoMoreData(part1, 900, 1000, true);
    provider.releaseOwnedTable(part1, threadNumber);
    assertThat(activePartitions.size(), equalTo(1));

    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(true));
    assertThat(provider.getSharedAvailableTablesList(), empty());
    assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));

  }

  @Test
  public void switchFromPartitionedToNotPartitioned() throws InterruptedException, StageException {
    int batchSize = 10;
    String schema = "db";
    String table1Name = "table1";
    String offsetCol = "col";
    final String partitionSize = "100";
    int maxActivePartitions = 3;
    int threadNumber = 0;
    int numThreads = 1;

    TableContext table1 = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, true);

    MultithreadedTableProvider provider = createTableProvider(numThreads, table1, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);

    TableRuntimeContext part1 = provider.nextTable(threadNumber);
    part1.recordColumnOffset(offsetCol, "0");
    part1.recordColumnOffset(offsetCol, "1");
    provider.releaseOwnedTable(part1, threadNumber);
    part1 = provider.nextTable(threadNumber);
    provider.releaseOwnedTable(part1, threadNumber);
    TableRuntimeContext part2 = provider.nextTable(threadNumber);
    part2.recordColumnOffset(offsetCol, "101");
    provider.releaseOwnedTable(part2, threadNumber);
    TableRuntimeContext part3 = provider.nextTable(threadNumber);
    part3.recordColumnOffset(offsetCol, "201");
    provider.releaseOwnedTable(part3, threadNumber);

    // now, "stop" producing and capture current offsets
    final Map<String, String> currentOffsets = new HashMap<>();
    for (TableRuntimeContext part : Arrays.asList(part1, part2, part3)) {
      // since we're not using a runner, just treat the partition's member variable for
      // starting position offsets as the runner's stored offsets, since for our purposes
      // in this test, it will work the same
      final Map<String, String> partStartingOffsets = part.getPartitionOffsetStart();
      final String offsetRepresentation = OffsetQueryUtil.getSourceKeyOffsetsRepresentation(partStartingOffsets);
      currentOffsets.put(part.getOffsetKey(), offsetRepresentation);
    }

    // recreate the table as non-partitioned, and recreate the provider to use it
    TableContext table1NoPartitioning = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, false);
    provider = createTableProvider(numThreads, table1NoPartitioning, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);
    provider.initializeFromV2Offsets(currentOffsets, new HashMap<>());

    part1 = provider.nextTable(threadNumber);
    provider.reportDataOrNoMoreData(part1, 0, batchSize, true);
    part1.setResultSetProduced(true);
    provider.releaseOwnedTable(part1, threadNumber);
    validatePartition(part1, 1, table1NoPartitioning, false, true, true, true, offsetCol, partitionSize);

    part2 = provider.nextTable(threadNumber);
    provider.reportDataOrNoMoreData(part2, 0, batchSize, true);
    part2.setResultSetProduced(true);
    provider.releaseOwnedTable(part2, threadNumber);
    validatePartition(part2, 2, table1NoPartitioning, false, true, true, true, offsetCol, partitionSize);

    part3 = provider.nextTable(threadNumber);
    provider.reportDataOrNoMoreData(part3, 0, batchSize, true);
    part3.setResultSetProduced(true);
    provider.releaseOwnedTable(part3, threadNumber);
    validatePartition(part3, 3, table1NoPartitioning, false, true, true, true, offsetCol, partitionSize);

    TableRuntimeContext part4 = provider.nextTable(threadNumber);
    validatePartition(part4, 4, table1NoPartitioning, false, false, true, false, offsetCol, partitionSize, false);
    provider.releaseOwnedTable(part4, threadNumber);
  }

  @Test
  public void switchFromNotPartitionedToPartitioned() throws InterruptedException, StageException {
    int batchSize = 10;
    String schema = "db";
    String table1Name = "table1";
    String offsetCol = "col";
    final String partitionSize = "100";
    int maxActivePartitions = 3;
    int threadNumber = 0;
    int numThreads = 1;
    final String switchPartitionOnAtOffset = "3";

    TableContext table1 = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, false);

    MultithreadedTableProvider provider = createTableProvider(numThreads, table1, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);

    TableRuntimeContext part1 = provider.nextTable(threadNumber);

    validatePartition(
        part1,
        TableRuntimeContext.NON_PARTITIONED_SEQUENCE,
        table1,
        false,
        false,
        false,
        false,
        offsetCol,
        partitionSize,
        false
    );

    part1.recordColumnOffset(offsetCol, "0");
    assertThat(part1.isFirstRecordedOffsetsPassed(), equalTo(false));

    // simulate another row with offset 0
    part1.recordColumnOffset(offsetCol, "0");
    // and we still haven't passed beyond the initial offset value of 0
    assertThat(part1.isFirstRecordedOffsetsPassed(), equalTo(false));

    part1.recordColumnOffset(offsetCol, "1");
    // we have now passed the initial offset value of 0
    assertThat(part1.isFirstRecordedOffsetsPassed(), equalTo(true));

    // simulate another row with offset 1
    part1.recordColumnOffset(offsetCol, "1");
    // but the first recorded offset (0) has still passed
    assertThat(part1.isFirstRecordedOffsetsPassed(), equalTo(true));

    part1.recordColumnOffset(offsetCol, "2");

    part1.recordColumnOffset(offsetCol, switchPartitionOnAtOffset);

    provider.reportDataOrNoMoreData(part1, 4, batchSize, false);
    part1.setResultSetProduced(true);

    provider.releaseOwnedTable(part1, threadNumber);
    part1 = provider.nextTable(threadNumber);

    validatePartition(
        part1,
        TableRuntimeContext.NON_PARTITIONED_SEQUENCE,
        table1,
        true,
        true,
        false,
        false,
        offsetCol,
        partitionSize,
        false
    );

    final Map<String, String> runnerOffsets = new HashMap<>();
    final Map<String, String> part1Offsets = Collections.singletonMap(offsetCol, switchPartitionOnAtOffset);
    runnerOffsets.put(part1.getOffsetKey(), OffsetQueryUtil.getSourceKeyOffsetsRepresentation(part1Offsets));

    // now recreate the table as partitioned
    table1 = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, true);

    // ...and the runner
    provider = createTableProvider(numThreads, table1, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);
    provider.initializeFromV2Offsets(runnerOffsets, new HashMap<>());

    part1 = provider.nextTable(threadNumber);

    validatePartition(part1, 1, table1, false, false, true, false, offsetCol, partitionSize);

  }

  @Test
  public void addTableNotPartitioned() throws InterruptedException, StageException {
    String schema = "db";
    String table1Name = "table1";
    String table2Name = "table2";
    String offsetCol = null;
    final String partitionSize = null;
    int maxActivePartitions = 0;
    int threadNumber = 0;
    int numThreads = 1;

    TableContext table1 = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, true);

    MultithreadedTableProvider provider = createTableProvider(numThreads, table1, BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE);

    TableRuntimeContext tableRuntimeContext = provider.nextTable(threadNumber);
    Assert.equals(table1Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);

    tableRuntimeContext = provider.nextTable(threadNumber);
    Assert.equals(table1Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);

    TableContext table2 = createTableContext(schema, table2Name, offsetCol, partitionSize, maxActivePartitions, true);
    Map<String, TableContext> tableContextMap = new HashMap<>();

    tableContextMap.put(table1.getQualifiedName(), table1);
    tableContextMap.put(table2.getQualifiedName(), table2);
    Queue<String> sortedTableOrder = new LinkedList<>();
    sortedTableOrder.add(table1.getQualifiedName());
    sortedTableOrder.add(table2.getQualifiedName());

    //Set added table lists
    provider.setTableContextMap(tableContextMap, sortedTableOrder);

    tableRuntimeContext = provider.nextTable(threadNumber);

    Assert.equals(table1Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);

    tableRuntimeContext = provider.nextTable(threadNumber);
    Assert.equals(table2Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);
  }

  @Test
  public void removeTableNotPartitioned() throws InterruptedException, StageException {
    String schema = "db";
    String table1Name = "table1";
    String table2Name = "table2";
    String offsetCol = null;
    final String partitionSize = null;
    int maxActivePartitions = 0;
    int threadNumber = 0;
    int numThreads = 1;

    TableContext table1 = createTableContext(schema, table1Name, offsetCol, partitionSize, maxActivePartitions, true);
    TableContext table2 = createTableContext(schema, table2Name, offsetCol, partitionSize, maxActivePartitions, true);
    Map<String, TableContext> tableContextMap = new HashMap<>();

    tableContextMap.put(table1.getQualifiedName(), table1);
    tableContextMap.put(table2.getQualifiedName(), table2);
    Queue<String> sortedTableOrder = new LinkedList<>();
    sortedTableOrder.add(table1.getQualifiedName());
    sortedTableOrder.add(table2.getQualifiedName());

    Map threadNumToMaxTableSlots = new HashMap<>();

    BatchTableStrategy batchTableStrategy = BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE;
    MultithreadedTableProvider provider = new MultithreadedTableProvider(
        tableContextMap,
        sortedTableOrder,
        threadNumToMaxTableSlots,
        numThreads,
        batchTableStrategy,
        (ctx) -> {} // do-nothing implementation
    );

    TableRuntimeContext tableRuntimeContext = provider.nextTable(threadNumber);
    Assert.equals(table1Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);

    tableRuntimeContext = provider.nextTable(threadNumber);
    Assert.equals(table2Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);

    tableContextMap.remove(table2.getQualifiedName());
    sortedTableOrder.remove(table2.getQualifiedName());
    //Set removed table lists
    provider.setTableContextMap(tableContextMap, sortedTableOrder);

    tableRuntimeContext = provider.nextTable(threadNumber);

    Assert.equals(table1Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);

    tableRuntimeContext = provider.nextTable(threadNumber);
    Assert.equals(table1Name, tableRuntimeContext.getSourceTableContext().getTableName());
    provider.releaseOwnedTable(tableRuntimeContext, threadNumber);
  }

  @Test
  public void tableAndSchemasFinished() throws InterruptedException, StageException {
    String schema1 = "schema1";
    String table1Name = "table1";
    String table2Name = "table2";
    String schema2 = "schema2";
    String table3Name = "table3";

    String offsetCol = null;
    final String partitionSize = null;
    int maxActivePartitions = 0;
    int threadNumber = 0;
    int numThreads = 1;

    TableContext tableContext1 = createTableContext(schema1, table1Name, offsetCol, partitionSize, maxActivePartitions, false);
    TableContext tableContext2 = createTableContext(schema1, table2Name, offsetCol, partitionSize, maxActivePartitions, false);
    TableContext tableContext3 = createTableContext(schema2, table3Name, offsetCol, partitionSize, maxActivePartitions, false);

    Map<String, TableContext> tableContextMap = new HashMap<>();

    tableContextMap.put(tableContext1.getQualifiedName(), tableContext1);
    tableContextMap.put(tableContext2.getQualifiedName(), tableContext2);
    tableContextMap.put(tableContext3.getQualifiedName(), tableContext3);
    Queue<String> sortedTableOrder = new LinkedList<>();

    sortedTableOrder.add(tableContext1.getQualifiedName());
    sortedTableOrder.add(tableContext2.getQualifiedName());
    sortedTableOrder.add(tableContext3.getQualifiedName());

    Map threadNumToMaxTableSlots = new HashMap<>();

    BatchTableStrategy batchTableStrategy = BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE;
    MultithreadedTableProvider provider = new MultithreadedTableProvider(
        tableContextMap,
        sortedTableOrder,
        threadNumToMaxTableSlots,
        numThreads,
        batchTableStrategy,
        (ctx) -> {} // do-nothing implementation
    );

    assertThat(provider.getRemainingSchemasToTableContexts().size(), equalTo(3));

    TableRuntimeContext table1 = provider.nextTable(threadNumber);
    Assert.equals(table1Name, table1.getSourceTableContext().getTableName());

    assertThat(provider.getRemainingSchemasToTableContexts().size(), equalTo(3));
    // there should be two tables remaining in schema1 (table1 and table2)
    assertThat(provider.getRemainingSchemasToTableContexts().get(schema1).size(), equalTo(2));
    // and one remaining in schema2 (table3)
    assertThat(provider.getRemainingSchemasToTableContexts().get(schema2).size(), equalTo(1));

    final AtomicBoolean tableFinished = new AtomicBoolean(false);
    final AtomicBoolean schemaFinished = new AtomicBoolean(false);
    final List<String> schemaFinishedTables = new LinkedList<>();

    // finish table1
    provider.reportDataOrNoMoreData(table1, 10, 10, true, tableFinished, schemaFinished, schemaFinishedTables);

    // table should be finished
    assertTrue(tableFinished.get());

    // schema should not
    assertFalse(schemaFinished.get());
    assertThat(schemaFinishedTables, empty());
    assertThat(provider.getTablesWithNoMoreData().size(), equalTo(1));

    // there should be a total of two remaining entries in the map
    assertThat(provider.getRemainingSchemasToTableContexts().size(), equalTo(2));
    // one of which is in schema1
    assertThat(provider.getRemainingSchemasToTableContexts().get(schema1).size(), equalTo(1));
    // and one of which is in schema2
    assertThat(provider.getRemainingSchemasToTableContexts().get(schema2).size(), equalTo(1));

    provider.releaseOwnedTable(table1, 1);
    tableFinished.set(false);
    schemaFinished.set(false);
    schemaFinishedTables.clear();

    TableRuntimeContext table2 = provider.nextTable(threadNumber);
    Assert.equals(table2Name, table2.getSourceTableContext().getTableName());

    // finish table2
    provider.reportDataOrNoMoreData(table2, 10, 10, true, tableFinished, schemaFinished, schemaFinishedTables);

    // table should be finished
    assertTrue(tableFinished.get());
    // as should the schema this time
    assertTrue(schemaFinished.get());
    assertThat(schemaFinishedTables, hasSize(2));
    assertThat(provider.getTablesWithNoMoreData().size(), equalTo(2));
    // there should only be one entry left now
    assertThat(provider.getRemainingSchemasToTableContexts().size(), equalTo(1));
    assertTrue(provider.getRemainingSchemasToTableContexts().get(schema1).isEmpty());
    // which is for schema2
    assertThat(provider.getRemainingSchemasToTableContexts().get(schema2).size(), equalTo(1));

    provider.releaseOwnedTable(table2, 1);
    tableFinished.set(false);
    schemaFinished.set(false);
    schemaFinishedTables.clear();

    TableRuntimeContext table3 = provider.nextTable(threadNumber);
    Assert.equals(table3Name, table3.getSourceTableContext().getTableName());

    // suppose we did NOT actually reach the end of table3, in which case the conditions should be the same as above
    provider.reportDataOrNoMoreData(table3, 10, 10, false, tableFinished, schemaFinished, schemaFinishedTables);

    // now neither the table
    assertFalse(tableFinished.get());
    // nor schema should be finished
    assertFalse(schemaFinished.get());
    assertThat(schemaFinishedTables, empty());
    // and entries in the map should be the same as above
    assertThat(provider.getTablesWithNoMoreData().size(), equalTo(2));
    assertThat(provider.getRemainingSchemasToTableContexts().size(), equalTo(1));
    assertTrue(provider.getRemainingSchemasToTableContexts().get(schema1).isEmpty());

    provider.releaseOwnedTable(table3, 1);
    tableFinished.set(false);
    schemaFinished.set(false);
    schemaFinishedTables.clear();

    // cycle through table1 and table2 again
    table1 = provider.nextTable(threadNumber);
    provider.releaseOwnedTable(table1, 1);
    table2 = provider.nextTable(threadNumber);
    provider.releaseOwnedTable(table2, 1);

    // and get back to table3
    table3 = provider.nextTable(threadNumber);
    Assert.equals(table3Name, table3.getSourceTableContext().getTableName());

    // now suppose we have finally finished table3
    provider.reportDataOrNoMoreData(table3, 3, 10, true, tableFinished, schemaFinished, schemaFinishedTables);

    // both table
    assertTrue(tableFinished.get());
    // and schema should be finished
    assertTrue(schemaFinished.get());
    assertThat(schemaFinishedTables, hasSize(1));
    assertThat(provider.getTablesWithNoMoreData().size(), equalTo(3));
    // there should now be no more entries in this map
    assertTrue(provider.getRemainingSchemasToTableContexts().isEmpty());

    provider.releaseOwnedTable(table3, 1);

    assertTrue(provider.shouldGenerateNoMoreDataEvent());


  }

  @Test
  public void restoreFromV1Offsets() throws InterruptedException, StageException {
    Map<TableRuntimeContext, Map<String, String>> partitionsAndOffsets = createRandomPartitionsAndStoredOffsets(false);

    final Map<String, String> offsets = new HashMap<>();
    for (Map.Entry<TableRuntimeContext, Map<String, String>> entry : partitionsAndOffsets.entrySet()) {
      final TableContext table = entry.getKey().getSourceTableContext();
      offsets.put(table.getQualifiedName(), OffsetQueryUtil.getOffsetFormat(entry.getValue()));
    }

    MultithreadedTableProvider provider = createProvider(partitionsAndOffsets.keySet());

    provider.initializeFromV1Offsets(offsets);

    assertLoadedPartitions(partitionsAndOffsets, provider);
  }

  @Test
  public void restoreFromV2Offsets() throws InterruptedException, StageException {
    Map<TableRuntimeContext, Map<String, String>> partitionsAndOffsets = createRandomPartitionsAndStoredOffsets(true);

    final Map<String, String> offsets = buildOffsetMap(partitionsAndOffsets);

    MultithreadedTableProvider provider = createProvider(partitionsAndOffsets.keySet());

    final HashMap<String, String> newCommitOffsets = new HashMap<>();
    provider.initializeFromV2Offsets(offsets, newCommitOffsets);
    assertThat(newCommitOffsets.size(), equalTo(offsets.size()));

    assertLoadedPartitions(partitionsAndOffsets, provider);

    // now test when the offset format is from before non-incremental mode was added
    provider = createProvider(partitionsAndOffsets.keySet());
    final HashMap<String, String> newCommitOffsetsPreNonInc = new HashMap<>();
    final Map<String, String> preNonIncrementalOffsets = buildOffsetMap(partitionsAndOffsets, true);
    provider.initializeFromV2Offsets(preNonIncrementalOffsets, newCommitOffsetsPreNonInc);
    assertThat(newCommitOffsetsPreNonInc.size(), equalTo(preNonIncrementalOffsets.size()));
    assertThat(newCommitOffsetsPreNonInc, equalTo(newCommitOffsets));

    assertLoadedPartitions(partitionsAndOffsets, provider);
  }

  @Test
  public void tableWithNoMinOffsetValues() {
    TableContext table = createTableContext(
        "schema",
        "tableName",
        "off",
        "1000",
        null,
        -1,
        true
    );

    List<String> reasons = new LinkedList<>();
    assertThat(TableContext.isPartitionable(table, reasons), equalTo(false));
    assertThat(reasons, hasSize(1));
    assertThat(reasons.get(0), containsString("did not have a minimum value available"));
  }

  @Test
  public void tableWithSingleOffsetValue() {
    long offset = 1000;
    TableContext table = createTableContext(
        "schema",
        "tableName",
        "off",
        "1000",
        null,
        -1,
        true,
        false,
        offset
    );

    assertEquals(offset, table.getOffset());
  }

}
