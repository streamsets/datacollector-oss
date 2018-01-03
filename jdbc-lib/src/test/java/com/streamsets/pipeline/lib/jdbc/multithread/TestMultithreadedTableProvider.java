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
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.streamsets.testing.RandomTestUtils;
import com.vividsolutions.jts.util.Assert;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class TestMultithreadedTableProvider {

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
    assertThat(provider.getSharedAvailableTablesQueue(), empty());
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
      final Map<String, String> partStartingOffsets = part.getStartingPartitionOffsets();
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
        true,
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

    Map<String, String> expectedStartOffsets = part1Offsets;
    Map<String, String> expectedMaxOffsets = new HashMap<>();
    expectedMaxOffsets.put(offsetCol, part1.generateNextPartitionOffset(offsetCol, switchPartitionOnAtOffset));

    validatePartition(
        part1,
        1,
        table1,
        false,
        false,
        true,
        false,
        offsetCol,
        partitionSize,
        true,
        expectedStartOffsets,
        expectedMaxOffsets
    );

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
        batchTableStrategy
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

  private static void validatePartition(
      TableRuntimeContext part,
      int partitionSequence,
      TableContext table,
      boolean offsetsRecorded,
      boolean resultSetProduced,
      boolean expectOffsets,
      boolean expectNoMoreData,
      String offsetColumn,
      String partitionSize
  ) {
    validatePartition(
        part,
        partitionSequence,
        table,
        offsetsRecorded,
        resultSetProduced,
        expectOffsets,
        expectNoMoreData,
        offsetColumn,
        partitionSize,
        true,
        null,
        null
    );
  }

  private static void validatePartition(
      TableRuntimeContext part,
      int partitionSequence,
      TableContext table,
      boolean offsetsRecorded,
      boolean resultSetProduced,
      boolean expectOffsets,
      boolean expectNoMoreData,
      String offsetColumn,
      String partitionSize,
      boolean partitioned
  ) {
    validatePartition(
        part,
        partitionSequence,
        table,
        offsetsRecorded,
        resultSetProduced,
        expectOffsets,
        expectNoMoreData,
        offsetColumn,
        partitionSize,
        partitioned,
        null,
        null
    );
  }

  private static void validatePartition(
      TableRuntimeContext part,
      int partitionSequence,
      TableContext table,
      boolean offsetsRecorded,
      boolean resultSetProduced,
      boolean expectOffsets,
      boolean expectNoMoreData,
      String offsetColumn,
      String partitionSize,
      boolean partitioned,
      Map<String, String> expectedMinOffsets,
      Map<String, String> expectedMaxOffsets
  ) {

    assertThat(part.getPartitionSequence(), equalTo(partitionSequence));
    assertThat(part.isAnyOffsetsRecorded(), equalTo(offsetsRecorded));
    assertThat(part.isResultSetProduced(), equalTo(resultSetProduced));
    assertThat(part.getSourceTableContext(), equalTo(table));
    assertThat(part.isMarkedNoMoreData(), equalTo(expectNoMoreData));
    assertThat(part.isPartitioned(), equalTo(partitioned));
    assertThat(part.getQualifiedName(), equalTo(table.getQualifiedName()));

    if (partitioned) {
      if (expectOffsets) {
        if (expectedMinOffsets != null) {
          assertThat(part.getStartingPartitionOffsets(), equalTo(expectedMinOffsets));
        } else {
          int expectedMinOffset = (part.getPartitionSequence() - 1) * Integer.parseInt(partitionSize);
          assertThat(part.getStartingPartitionOffsets().size(), equalTo(1));
          assertThat(part.getStartingPartitionOffsets(), hasKey(offsetColumn));
          assertThat(part.getStartingPartitionOffsets().get(offsetColumn), equalTo(String.valueOf(expectedMinOffset)));
        }

        if (expectedMaxOffsets != null) {
          assertThat(part.getMaxPartitionOffsets(), equalTo(expectedMaxOffsets));
        } else {
          int expectedMaxOffset = part.getPartitionSequence() * Integer.parseInt(partitionSize);
          assertThat(part.getMaxPartitionOffsets().size(), equalTo(1));
          assertThat(part.getMaxPartitionOffsets(), hasKey(offsetColumn));
          assertThat(part.getMaxPartitionOffsets().get(offsetColumn), equalTo(String.valueOf(expectedMaxOffset)));
        }
      }
    } else {
      if (expectOffsets) {
        assertThat(part.getStartingPartitionOffsets().size(), equalTo(1));
        assertThat(part.getStartingPartitionOffsets(), hasKey(offsetColumn));
        assertThat(part.getMaxPartitionOffsets().size(), equalTo(0));
      }
    }

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

  private void assertLoadedPartitions(
      Map<TableRuntimeContext, Map<String, String>> partitionsAndOffsets,
      MultithreadedTableProvider provider
  ) {
    Map<TableRuntimeContext, Map<String, String>> checkPartitionsAndOffsets = new HashMap<>(partitionsAndOffsets);
    final SortedSetMultimap<TableContext, TableRuntimeContext> activePartitions = provider.getActiveRuntimeContexts();

    Map<TableContext, Integer> maxPartitionSequenceWithData = new HashMap<>();
    Map<TableContext, Integer> minPartitionSequenceSeen = new HashMap<>();

    for (Map.Entry<TableContext, TableRuntimeContext> partitionEntry : activePartitions.entries()) {
      TableRuntimeContext partition = partitionEntry.getValue();
      int sequence = partition.getPartitionSequence();
      TableContext table = partition.getSourceTableContext();
      if (partition.isPartitioned()
          && (!minPartitionSequenceSeen.containsKey(table) || minPartitionSequenceSeen.get(table) > sequence)) {
        minPartitionSequenceSeen.put(table, sequence);
      }

      assertThat(
          "partitionsAndOffsets did not contain a key seen in provider activeRuntimeContexts",
          checkPartitionsAndOffsets,
          hasKey(partition)
      );
      Map<String, String> storedOffsets = checkPartitionsAndOffsets.remove(partition);
      if (storedOffsets == null) {
        assertThat(
            "partition initialStoredOffsets should have been empty",
            partition.getInitialStoredOffsets().size(),
            equalTo(0)
        );
      } else {
        assertThat(
            "partition initialStoredOffsets did not match randomly generated values",
            storedOffsets,
            equalTo(partition.getInitialStoredOffsets())
        );
        if (partition.isPartitioned()) {
          if (!maxPartitionSequenceWithData.containsKey(table)
              || maxPartitionSequenceWithData.get(table) < sequence) {
            maxPartitionSequenceWithData.put(table, sequence);
          }
        }
      }
    }
    assertThat(
        "randomly generated partition in partitionsAndOffsets did not appear in provider activeRuntimeContexts",
        checkPartitionsAndOffsets.size(),
        equalTo(0)
    );
    for (Map.Entry<TableContext, Integer> minSequenceSeenEntry : minPartitionSequenceSeen.entrySet()) {
      final TableContext table = minSequenceSeenEntry.getKey();
      if (!maxPartitionSequenceWithData.containsKey(table) && table.isPartitionable()) {
        maxPartitionSequenceWithData.put(table, minSequenceSeenEntry.getValue() - 1);
      }
    }
    assertThat(
      "",
      provider.getMaxPartitionWithDataPerTable(),
      equalTo(new ConcurrentHashMap<>(maxPartitionSequenceWithData))
    );
  }

  private static MultithreadedTableProvider createProvider(Collection<TableRuntimeContext> partitions) {

    Map<String, TableContext> tableContexts = new HashMap<>();
    Queue<String> tableOrder = new LinkedList<>();
    for (TableRuntimeContext partition : partitions) {
      TableContext table = partition.getSourceTableContext();
      final String tableName = table.getQualifiedName();
      tableContexts.put(tableName, table);
      tableOrder.add(tableName);
    }

    return new MultithreadedTableProvider(
        tableContexts,
        tableOrder,
        Collections.emptyMap(),
        1,
        BatchTableStrategy.SWITCH_TABLES
    );
  }

  private static Map<String, String> buildOffsetMap(Map<TableRuntimeContext, Map<String, String>> partitions) {
    return buildOffsetMap(partitions, false);
  }

  private static Map<String, String> buildOffsetMap(
      Map<TableRuntimeContext, Map<String, String>> partitions,
      final boolean preNonIncremental
  ) {
    final Map<String, String> offsets = new HashMap<>();
    partitions.forEach((part, off) -> {
      String offsetKey = part.getOffsetKey();
      if (preNonIncremental) {
        // before non-incremental mode, offset keys didn't have the final portion (usingNonIncrementalLoad)
        // so remove the final term
        final int index = StringUtils.lastIndexOf(offsetKey, TableRuntimeContext.OFFSET_TERM_SEPARATOR);
        offsetKey = offsetKey.substring(0, index);
      }
      offsets.put(offsetKey, off == null ? null : OffsetQueryUtil.getOffsetFormat(off));
    });
    return offsets;
  }

  private static Matcher<Map<String, String>> offsetMapOf(String column, String offset) {
    return new BaseMatcher<Map<String,String>>() {
      private final Map<String, String> expectedMap = Collections.singletonMap(column, offset);

      @Override
      public boolean matches(Object item) {
        return expectedMap.equals(item);
      }

      @Override
      public void describeTo(Description description) {
        description.appendValue(expectedMap);
      }
    };
  }

  private static Map<TableRuntimeContext, Map<String, String>> createRandomPartitionsAndStoredOffsets(
      boolean enablePartitioning
  ) {
    Random random = RandomTestUtils.getRandom();
    Map<TableRuntimeContext, Map<String, String>> partitions = new HashMap<>();

    List<Integer> sqlTypes = new ArrayList<>(TableContextUtil.PARTITIONABLE_TYPES);

    String schemaName = "schema";
    String offsetColName = "OFFSET_COL";

    int numTables = RandomTestUtils.nextInt(1, 8);
    for (int t = 0; t < numTables; t++) {
      String tableName = String.format("table%d", t);

      int type = sqlTypes.get(RandomTestUtils.nextInt(0, sqlTypes.size()));
      PartitioningMode partitioningMode = enablePartitioning && random.nextBoolean()
          ? PartitioningMode.BEST_EFFORT : PartitioningMode.DISABLED;
      final boolean partitioned = partitioningMode == PartitioningMode.BEST_EFFORT;
      int maxNumPartitions = partitioned ? RandomTestUtils.nextInt(1, 10) : 1;

      // an integer should be compatible with all partitionable types
      int partitionSize = RandomTestUtils.nextInt(1, 1000000);

      TableContext table = new TableContext(
          schemaName,
          tableName,
          Maps.newLinkedHashMap(Collections.singletonMap(offsetColName, type)),
          Collections.singletonMap(offsetColName, null),
          Collections.singletonMap(offsetColName, String.valueOf(partitionSize)),
          Collections.singletonMap(offsetColName, "0"),
          TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
          partitioningMode,
          maxNumPartitions,
          null
      );

      for (int p = 0; p < maxNumPartitions; p++) {
        if (partitioned && random.nextBoolean() && !(p == maxNumPartitions - 1 && partitions.isEmpty())) {
          // only create some partitions
          continue;
        }

        int startOffset = p * partitionSize;
        int maxOffset = (p + 1) * partitionSize;

        Map<String, String> partitionStoredOffsets = null;
        if (random.nextBoolean()) {
          // only simulate stored offsets sometimes
          int storedOffset = RandomTestUtils.nextInt(startOffset + 1, maxOffset + 1);
          partitionStoredOffsets = Collections.singletonMap(offsetColName, String.valueOf(storedOffset));
        }

        TableRuntimeContext partition = new TableRuntimeContext(
            table,
            false,
            partitioned,
            partitioned ? p + 1 : TableRuntimeContext.NON_PARTITIONED_SEQUENCE,
            Collections.singletonMap(offsetColName, String.valueOf(startOffset)),
            Collections.singletonMap(offsetColName, String.valueOf(maxOffset)),
            partitionStoredOffsets
        );

        partitions.put(partition, partitionStoredOffsets);
      }
    }

    return partitions;
  }

  @NotNull
  private MultithreadedTableProvider createTableProvider(
      int numThreads,
      TableContext table,
      BatchTableStrategy batchTableStrategy
  ) {
    Map<String, TableContext> tableContextMap = new HashMap<>();
    String qualifiedName = table.getQualifiedName();
    tableContextMap.put(qualifiedName, table);
    Queue<String> sortedTableOrder = new LinkedList<>();
    sortedTableOrder.add(qualifiedName);
    Map<Integer, Integer> threadNumToMaxTableSlots = new HashMap<>();

    return new MultithreadedTableProvider(
        tableContextMap,
        sortedTableOrder,
        threadNumToMaxTableSlots,
        numThreads,
        batchTableStrategy
    );
  }

  @NotNull
  private static TableContext createTableContext(
      String schema,
      String tableName,
      String offsetColumn,
      String partitionSize,
      int maxActivePartitions,
      boolean enablePartitioning
  ) {
    return createTableContext(
        schema,
        tableName,
        offsetColumn,
        partitionSize,
        "0",
        maxActivePartitions,
        enablePartitioning
    );
  }

  @NotNull
  private static TableContext createTableContext(
      String schema,
      String tableName,
      String offsetColumn,
      String partitionSize,
      String minOffsetColValue,
      int maxActivePartitions,
      boolean enablePartitioning
  ) {
    return createTableContext(
        schema,
        tableName,
        offsetColumn,
        partitionSize,
        minOffsetColValue,
        maxActivePartitions,
        enablePartitioning,
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE
    );
  }

  @NotNull
  private static TableContext createTableContext(
      String schema,
      String tableName,
      String offsetColumn,
      String partitionSize,
      String minOffsetColValue,
      int maxActivePartitions,
      boolean enablePartitioning,
      boolean enableNonIncremental
  ) {
    LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
    Map<String, String> offsetColumnToStartOffset = new HashMap<>();
    Map<String, String> offsetColumnToPartitionSizes = new HashMap<>();
    Map<String, String> offsetColumnToMinValues = new HashMap<>();

    if (offsetColumn != null) {
      offsetColumnToType.put(offsetColumn, Types.INTEGER);
      if (minOffsetColValue != null) {
        offsetColumnToMinValues.put(offsetColumn, minOffsetColValue);
      }
      offsetColumnToPartitionSizes.put(offsetColumn, partitionSize);
    }
    String extraOffsetColumnConditions = null;

    return new TableContext(
        schema,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetColumnToPartitionSizes,
        offsetColumnToMinValues,
        enableNonIncremental,
        enablePartitioning ? PartitioningMode.BEST_EFFORT : PartitioningMode.DISABLED,
        maxActivePartitions,
        extraOffsetColumnConditions
    );
  }

}
