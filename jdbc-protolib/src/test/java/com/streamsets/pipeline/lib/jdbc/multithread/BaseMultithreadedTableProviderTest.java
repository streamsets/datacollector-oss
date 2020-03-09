/*
 * Copyright 2019 StreamSets Inc.
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
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.streamsets.testing.RandomTestUtils;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.NotNull;

import java.sql.Types;
import java.util.ArrayList;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;

public abstract class BaseMultithreadedTableProviderTest {
  protected static void validatePartition(
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

  protected static void validatePartition(
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

  protected static void validatePartition(
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
          assertThat(part.getPartitionOffsetStart(), equalTo(expectedMinOffsets));
        } else {
          int expectedMinOffset = (part.getPartitionSequence() - 1) * Integer.parseInt(partitionSize);
          assertThat(part.getPartitionOffsetStart().size(), equalTo(1));
          assertThat(part.getPartitionOffsetStart(), hasKey(offsetColumn));
          assertThat(part.getPartitionOffsetStart().get(offsetColumn), equalTo(String.valueOf(expectedMinOffset)));
        }

        if (expectedMaxOffsets != null) {
          assertThat(part.getPartitionOffsetEnd(), equalTo(expectedMaxOffsets));
        } else {
          int expectedMaxOffset = part.getPartitionSequence() * Integer.parseInt(partitionSize);
          assertThat(part.getPartitionOffsetEnd().size(), equalTo(1));
          assertThat(part.getPartitionOffsetEnd(), hasKey(offsetColumn));
          assertThat(part.getPartitionOffsetEnd().get(offsetColumn), equalTo(String.valueOf(expectedMaxOffset)));
        }
      }
    } else {
      if (expectOffsets) {
        assertThat(part.getPartitionOffsetStart().size(), equalTo(1));
        assertThat(part.getPartitionOffsetStart(), hasKey(offsetColumn));
        assertThat(part.getPartitionOffsetEnd().size(), equalTo(0));
      }
    }

  }

  protected static MultithreadedTableProvider createProvider(Collection<TableRuntimeContext> partitions) {

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
        BatchTableStrategy.SWITCH_TABLES,
        (ctx) -> {} // do-nothing implementation
    );
  }

  protected static Map<String, String> buildOffsetMap(Map<TableRuntimeContext, Map<String, String>> partitions) {
    return buildOffsetMap(partitions, false);
  }

  protected static Map<String, String> buildOffsetMap(
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

  protected static Map<TableRuntimeContext, Map<String, String>> createRandomPartitionsAndStoredOffsets(
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
          DatabaseVendor.UNKNOWN,
          QuoteChar.NONE,
          schemaName,
          tableName,
          Maps.newLinkedHashMap(Collections.singletonMap(offsetColName, type)),
          Collections.emptyMap(),
          Collections.singletonMap(offsetColName, String.valueOf(partitionSize)),
          Collections.singletonMap(offsetColName, "0"),
          Collections.emptyMap(),
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
  protected static TableContext createTableContext(
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
  protected static TableContext createTableContext(
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
        false,
        new Long(0)
    );
  }

  @NotNull
  protected static TableContext createTableContext(
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
    Map<String, String> offsetColumnToMaxValues = new HashMap<>();

    if (offsetColumn != null) {
      offsetColumnToType.put(offsetColumn, Types.INTEGER);
      if (minOffsetColValue != null) {
        offsetColumnToMinValues.put(offsetColumn, minOffsetColValue);
      }
      offsetColumnToPartitionSizes.put(offsetColumn, partitionSize);
    }
    String extraOffsetColumnConditions = null;

    return new TableContext(
        DatabaseVendor.UNKNOWN,
        QuoteChar.NONE,
        schema,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetColumnToPartitionSizes,
        offsetColumnToMinValues,
        offsetColumnToMaxValues,
        enableNonIncremental,
        enablePartitioning ? PartitioningMode.BEST_EFFORT : PartitioningMode.DISABLED,
        maxActivePartitions,
        extraOffsetColumnConditions
    );
  }

  @NotNull
  protected static TableContext createTableContext(
      String schema,
      String tableName,
      String offsetColumn,
      String partitionSize,
      String minOffsetColValue,
      int maxActivePartitions,
      boolean enablePartitioning,
      boolean enableNonIncremental,
      long offset
  ) {
    return createTableContext(
        schema,
        tableName,
        offsetColumn,
        partitionSize,
        minOffsetColValue,
        null,
        maxActivePartitions,
        enablePartitioning,
        enableNonIncremental,
        offset
    );
  }

  @NotNull
  protected static TableContext createTableContext(
      String schema,
      String tableName,
      String offsetColumn,
      String partitionSize,
      String minOffsetColValue,
      String maxOffsetColValue,
      int maxActivePartitions,
      boolean enablePartitioning,
      boolean enableNonIncremental,
      long offset
  ) {
    LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
    Map<String, String> offsetColumnToStartOffset = new HashMap<>();
    Map<String, String> offsetColumnToPartitionSizes = new HashMap<>();
    Map<String, String> offsetColumnToMinValues = new HashMap<>();
    Map<String, String> offsetColumnToMaxValues = new HashMap<>();

    if (offsetColumn != null) {
      offsetColumnToType.put(offsetColumn, Types.INTEGER);
      offsetColumnToPartitionSizes.put(offsetColumn, partitionSize);

      if (minOffsetColValue != null) {
        offsetColumnToMinValues.put(offsetColumn, minOffsetColValue);
      }
      if (maxOffsetColValue != null) {
        offsetColumnToMaxValues.put(offsetColumn, maxOffsetColValue);
      }
    }
    String extraOffsetColumnConditions = null;

    return new TableContext(
        DatabaseVendor.UNKNOWN,
        QuoteChar.NONE,
        schema,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetColumnToPartitionSizes,
        offsetColumnToMinValues,
        offsetColumnToMaxValues,
        enableNonIncremental,
        enablePartitioning ? PartitioningMode.BEST_EFFORT : PartitioningMode.DISABLED,
        maxActivePartitions,
        extraOffsetColumnConditions,
        offset
    );
  }

  protected void assertLoadedPartitions(
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

  @NotNull
  protected MultithreadedTableProvider createTableProvider(
      int numThreads,
      TableContext table,
      BatchTableStrategy batchTableStrategy
  ) {
    return createTableProvider(
        numThreads,
        table,
        batchTableStrategy,
        (ctx) -> {} // do-nothing implementation
    );
  }

  @NotNull
  protected MultithreadedTableProvider createTableProvider(
      int numThreads,
      TableContext table,
      BatchTableStrategy batchTableStrategy,
      TableMaxOffsetValueUpdater tableMaxOffsetValueUpdater
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
        batchTableStrategy,
        tableMaxOffsetValueUpdater
    );
  }
}
