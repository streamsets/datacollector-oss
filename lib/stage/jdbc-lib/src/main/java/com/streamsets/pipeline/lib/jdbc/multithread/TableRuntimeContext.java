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

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TableRuntimeContext {
  public static final int NON_PARTITIONED_SEQUENCE = -1;
  private static final Logger LOG = LoggerFactory.getLogger(TableRuntimeContext.class);

  public static final String TABLE_NAME_KEY = "tableName";
  public static final String PARTITIONED_KEY = "partitioned";
  public static final String PARTITION_SEQUENCE_KEY = "partitionSequence";
  public static final String PARTITION_START_OFFSETS_KEY = "partitionStartOffsets";
  public static final String PARTITION_MAX_OFFSETS_KEY = "partitionMaxOffsets";
  public static final String USING_NON_INCREMENTAL_LOAD_KEY = "usingNonIncrementalLoad";

  public static final String NON_INCREMENTAL_LOAD_OFFSET_COMPLETED_KEY = "completed";

  private final TableContext sourceTableContext;
  private final boolean partitioned;
  private final boolean usingNonIncrementalLoad;
  private final int partitionSequence;
  private final Map<String, String> startingPartitionOffsets = new HashMap<>();
  private final Map<String, String> maxPartitionOffsets = new HashMap<>();

  private final Map<String, String> initialStoredOffsets = new HashMap<>();

  private boolean anyOffsetsRecorded = false;

  private boolean markedNoMoreData = false;

  private boolean resultSetProduced = false;

  private Map<String, String> firstRecordedOffsets;
  private boolean firstRecordedOffsetsPassed = false;

  public static TableRuntimeContext createInitialPartition(
      TableContext sourceTableContext
  ) {
    return createInitialPartition(sourceTableContext, null);
  }

  public static TableRuntimeContext createInitialPartition(
      TableContext sourceTableContext,
      Map<String, String> storedOffsets
  ) {
    final boolean useNonIncrementalLoad = sourceTableContext.isNonIncrementalLoadRequired();
    if (sourceTableContext.getPartitioningMode() != PartitioningMode.DISABLED && sourceTableContext.isPartitionable()) {
      return new TableRuntimeContext(
          sourceTableContext,
          useNonIncrementalLoad,
          true,
          1,
          sourceTableContext.getOffsetColumnToStartOffset(),
          null,
          storedOffsets
      );
    } else {
      return new TableRuntimeContext(
          sourceTableContext,
          useNonIncrementalLoad,
          false,
          NON_PARTITIONED_SEQUENCE,
          sourceTableContext.getOffsetColumnToStartOffset(),
          null,
          storedOffsets
      );
    }
  }

  public TableRuntimeContext(
      TableContext sourceTableContext,
      boolean usingNonIncrementalLoad,
      boolean partitioned,
      int partitionSequence,
      Map<String, String> startingPartitionOffsets,
      Map<String, String> maxPartitionOffsets
  ) {
    this(
        sourceTableContext,
        usingNonIncrementalLoad,
        partitioned,
        partitionSequence,
        startingPartitionOffsets,
        maxPartitionOffsets,
        null
    );
  }

  TableRuntimeContext(
      TableContext sourceTableContext,
      boolean usingNonIncrementalLoad,
      boolean partitioned,
      int partitionSequence,
      Map<String, String> startingPartitionOffsets,
      Map<String, String> maxPartitionOffsets,
      Map<String, String> initialStoredOffsets
  ) {
    Utils.checkNotNull(sourceTableContext, "sourceTableContext");
    this.sourceTableContext = sourceTableContext;
    this.partitioned = partitioned;
    this.usingNonIncrementalLoad = usingNonIncrementalLoad;
    this.partitionSequence = partitionSequence;
    if (startingPartitionOffsets != null) {
      this.startingPartitionOffsets.putAll(startingPartitionOffsets);
    }
    if (maxPartitionOffsets != null) {
      this.maxPartitionOffsets.putAll(maxPartitionOffsets);
    }
    if (initialStoredOffsets != null) {
      this.initialStoredOffsets.putAll(initialStoredOffsets);
    }

    final Map<String, String> minOffsetValues = sourceTableContext.getOffsetColumnToMinValues();
    if (partitionSequence == 1 && this.startingPartitionOffsets.isEmpty() && !minOffsetValues.isEmpty()) {
      // we can use the min values to populate the starting and max partition offsets

      this.startingPartitionOffsets.putAll(minOffsetValues);
      this.startingPartitionOffsets.forEach((col, offset) -> this.maxPartitionOffsets.put(
          col,
          TableContextUtil.generateNextPartitionOffset(sourceTableContext, col, offset)
      ));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Table {} partition sequence {} created with offsets from {} to {}",
          sourceTableContext.getQualifiedName(),
          partitionSequence,
          startingPartitionOffsets,
          maxPartitionOffsets
      );
    }
  }

  public static SortedSetMultimap<TableContext, TableRuntimeContext> buildSortedPartitionMap() {
    return MultimapBuilder.hashKeys().treeSetValues(
        Comparator.comparingInt(TableRuntimeContext::getPartitionSequence)
    ).build();
  }

  public TableContext getSourceTableContext() {
    return sourceTableContext;
  }

  public boolean isPartitioned() {
    return partitioned;
  }

  public boolean isUsingNonIncrementalLoad() {
    return usingNonIncrementalLoad;
  }

  public int getPartitionSequence() {
    return partitionSequence;
  }

  public Map<String, String> getStartingPartitionOffsets() {
    return startingPartitionOffsets != null ? Collections.unmodifiableMap(startingPartitionOffsets) : null;
  }

  public Map<String, String> getMaxPartitionOffsets() {
    return maxPartitionOffsets != null ? Collections.unmodifiableMap(maxPartitionOffsets) : null;
  }

  public Map<String, String> getInitialStoredOffsets() {
    return initialStoredOffsets != null ? Collections.unmodifiableMap(initialStoredOffsets) : null;
  }

  private static String checkAndReturnOffsetTermValue(String term, String name, int position, String fullOffsetKey) {
    final String[] parts = StringUtils.splitByWholeSeparator(term, OFFSET_KEY_VALUE_SEPARATOR, 2);
    Utils.checkState(parts.length == 2, String.format(
        "Illegal offset term for %s (position %d separated by %s).  Should be in form %s%s<value>.  Full offset: %s",
        name,
        position,
        OFFSET_TERM_SEPARATOR,
        name,
        OFFSET_KEY_VALUE_SEPARATOR,
        fullOffsetKey
    ));
    return parts[1];
  }

  public static SortedSetMultimap<TableContext, TableRuntimeContext> initializeAndUpgradeFromV1Offsets(
      Map<String, TableContext> tableContextMap,
      Map<String, String> offsets,
      Set<String> offsetKeysToRemove
  ) throws StageException {
    SortedSetMultimap<TableContext, TableRuntimeContext> returnMap = buildSortedPartitionMap();

    for (Map.Entry<String, TableContext> tableEntry : tableContextMap.entrySet()) {
      final String tableName = tableEntry.getKey();
      final TableContext tableContext = tableEntry.getValue();

      Map<String, String> startingOffsets;
      String offsetValue = null;
      Map<String, String> storedOffsets = null;
      if (offsets.containsKey(tableName)) {
        offsetValue = offsets.remove(tableName);
        storedOffsets = OffsetQueryUtil.validateStoredAndSpecifiedOffset(tableContext, offsetValue);

        offsetKeysToRemove.add(tableName);

        startingOffsets = OffsetQueryUtil.getOffsetsFromSourceKeyRepresentation(offsetValue);
        tableContext.getOffsetColumnToStartOffset().putAll(startingOffsets);
      }

      final TableRuntimeContext partition = createInitialPartition(tableContext, storedOffsets);
      returnMap.put(tableContext, partition);

      if (offsetValue != null) {
        offsets.put(partition.getOffsetKey(), offsetValue);
      }
    }

    return returnMap;
  }

  public static SortedSetMultimap<TableContext, TableRuntimeContext> buildPartitionsFromStoredV2Offsets(
      Map<String, TableContext> tableContextMap,
      Map<String, String> offsets,
      Set<TableContext> excludeTables,
      Map<String, String> newCommitOffsets
  ) throws StageException {
    SortedSetMultimap<TableContext, TableRuntimeContext> returnMap = buildSortedPartitionMap();
    for (Map.Entry<String, String> offsetEntry : offsets.entrySet()) {
      final String offsetKey = offsetEntry.getKey();
      final String offsetValue = offsetEntry.getValue();
      LOG.debug("Parsing offset with key {}", offsetKey);
      final String[] parts = StringUtils.splitByWholeSeparator(offsetKey, OFFSET_TERM_SEPARATOR);
      if (parts.length < 5 || parts.length > 6) {
        throw new IllegalStateException(String.format(
            "Offset was not in correct format.  Expected 5 or 6 parts separated by %s to represent" +
                " %s, %s, %s, %s, %s, and - optionally - %s, respectively.  Invalid offset key: %s",
            OFFSET_TERM_SEPARATOR,
            TABLE_NAME_KEY,
            PARTITIONED_KEY,
            PARTITION_SEQUENCE_KEY,
            PARTITION_START_OFFSETS_KEY,
            PARTITION_MAX_OFFSETS_KEY,
            USING_NON_INCREMENTAL_LOAD_KEY,
            offsetKey)
        );
      }

      final String qualifiedTableName = checkAndReturnOffsetTermValue(
          parts[0],
          TABLE_NAME_KEY,
          1,
          offsetKey
      );

      final String partitionedStr = checkAndReturnOffsetTermValue(
          parts[1],
          PARTITIONED_KEY,
          2,
          offsetKey
      );
      final boolean partitioned = Boolean.valueOf(partitionedStr);
      final String partitionSequenceStr = checkAndReturnOffsetTermValue(
          parts[2],
          PARTITION_SEQUENCE_KEY,
          3,
          offsetKey
      );

      int partSeq;
      try {
        partSeq = Integer.parseInt(partitionSequenceStr);
      } catch (NumberFormatException e) {

        throw new IllegalStateException(String.format(
            "Illegal partitionSequence value (3rd term separated by %s) in stored offset key; should be integer: %s",
            OFFSET_TERM_SEPARATOR,
            offsetKey
        ), e);
      }

      final String partitionStartOffsetsStr = checkAndReturnOffsetTermValue(
          parts[3],
          PARTITION_START_OFFSETS_KEY,
          4,
          offsetKey
      );
      final Map<String, String> startOffsets = OffsetQueryUtil.getOffsetsFromSourceKeyRepresentation(
          partitionStartOffsetsStr
      );

      final String partitionMaxOffsetsStr = checkAndReturnOffsetTermValue(
          parts[4],
          PARTITION_MAX_OFFSETS_KEY,
          5,
          offsetKey
      );
      final Map<String, String> maxOffsets = OffsetQueryUtil.getOffsetsFromSourceKeyRepresentation(
          partitionMaxOffsetsStr
      );

      boolean usingNonIncrementalLoad = false;
      if (parts.length == 6) {
        // contains the non-incremental load key as well
        final String usingNonIncrementalLoadStr = checkAndReturnOffsetTermValue(
            parts[5],
            USING_NON_INCREMENTAL_LOAD_KEY,
            6,
            offsetKey
        );
        usingNonIncrementalLoad = BooleanUtils.toBoolean(usingNonIncrementalLoadStr);
      }

      // TODO: change code to read offset properly for non-incremental (finished=true) and NOT re-add if finished
      if (!tableContextMap.containsKey(qualifiedTableName)) {
        // TODO: something stronger here?  basically we will throw away an offset for a no-longer-configured table
        LOG.warn(String.format(
            "Ignoring offset for table (partitioned=%b) %s with sequence %d",
            partitioned,
            qualifiedTableName,
            partSeq
        ));
      } else {
        final TableContext tableContext = tableContextMap.get(qualifiedTableName);

        TableRuntimeContext partition = null;
        if (usingNonIncrementalLoad) {
          boolean completed = false;
          final Map<String, String> offsetMap = OffsetQueryUtil.getOffsetsFromSourceKeyRepresentation(offsetValue);
          final String checkStateMsg = String.format(
              "offset value for table using non-incremental load (key \"%s\") should be a map with at most a" +
                  " single key called %s (which has a boolean value), but was: %s",
              offsetKey,
              NON_INCREMENTAL_LOAD_OFFSET_COMPLETED_KEY,
              offsetValue
          );
          Utils.checkState(offsetMap.size() <= 1, checkStateMsg);
          if (!offsetMap.isEmpty()) {
            Utils.checkState(offsetMap.containsKey(NON_INCREMENTAL_LOAD_OFFSET_COMPLETED_KEY), checkStateMsg);
            completed = Boolean.valueOf(offsetMap.get(NON_INCREMENTAL_LOAD_OFFSET_COMPLETED_KEY));
          }

          if (completed) {
            LOG.info(
                "Table {} was marked completed by a non-incremental load, so it will not be added again unless the" +
                    " origin is reset",
                qualifiedTableName
            );
            excludeTables.add(tableContext);
          } else {
            partition = new TableRuntimeContext(
                tableContext,
                usingNonIncrementalLoad,
                partitioned,
                partSeq,
                startOffsets,
                maxOffsets,
                Collections.emptyMap()
            );
          }
        } else {
          final Map<String, String> initialStoredOffsets = OffsetQueryUtil.validateStoredAndSpecifiedOffset(
              tableContext,
              offsetValue
          );
          partition = new TableRuntimeContext(
              tableContext,
              usingNonIncrementalLoad,
              partitioned,
              partSeq,
              startOffsets,
              maxOffsets,
              initialStoredOffsets
          );
        }

        if (partition != null) {
          newCommitOffsets.put(partition.getOffsetKey(), offsetValue);
          returnMap.put(tableContext, partition);
        }
      }
    }

    return returnMap;
  }

  public static final String OFFSET_KEY_VALUE_SEPARATOR = "=";
  public static final String OFFSET_TERM_SEPARATOR = ";;;";

  private static final void appendOffsetTerm(StringBuilder sb, String name, String value, boolean last) {
    sb.append(name);
    sb.append(OFFSET_KEY_VALUE_SEPARATOR);
    sb.append(value);
    if (!last) {
      sb.append(OFFSET_TERM_SEPARATOR);
    }
  }

  public String getOffsetKey() {
    final StringBuilder sb = new StringBuilder();
    appendOffsetTerm(sb, TABLE_NAME_KEY, sourceTableContext.getQualifiedName(), false);
    appendOffsetTerm(sb, PARTITIONED_KEY, String.valueOf(isPartitioned()), false);
    appendOffsetTerm(sb, PARTITION_SEQUENCE_KEY, String.valueOf(getPartitionSequence()), false);
    appendOffsetTerm(
        sb,
        PARTITION_START_OFFSETS_KEY,
        OffsetQueryUtil.getSourceKeyOffsetsRepresentation(startingPartitionOffsets),
        false
    );
    appendOffsetTerm(
        sb,
        PARTITION_MAX_OFFSETS_KEY,
        OffsetQueryUtil.getSourceKeyOffsetsRepresentation(maxPartitionOffsets),
        false
    );
    appendOffsetTerm(
        sb,
        USING_NON_INCREMENTAL_LOAD_KEY,
        String.valueOf(usingNonIncrementalLoad),
        true
    );
    return sb.toString();
  }

  public static TableRuntimeContext createNextPartition(final TableRuntimeContext lastPartition) {
    if (!lastPartition.isPartitioned()) {
      throw new IllegalStateException("lastPartition TableRuntimeContext was not partitioned");
    }

    final Set<String> offsetColumns = lastPartition.getSourceTableContext().getOffsetColumns();
    final Map<String, String> startingPartitionOffsets = lastPartition.getStartingPartitionOffsets();

    if (startingPartitionOffsets.size() < offsetColumns.size()) {
      // we have not yet captured an offset for every offset columns
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Cannot create next partition after {} since we are missing values for offset columns {}",
            lastPartition.getPartitionSequence(),
            Sets.difference(offsetColumns, startingPartitionOffsets.keySet())
        );
      }
      return null;
    }

    final Map<String, String> nextStartingOffsets = new HashMap<>();
    final Map<String, String> nextMaxOffsets = new HashMap<>();

    final int newPartitionSequence = lastPartition.partitionSequence > 0 ? lastPartition.partitionSequence + 1 : 1;

    lastPartition.startingPartitionOffsets.forEach(
        (col, off) -> {
          String basedOnStartOffset = lastPartition.generateNextPartitionOffset(col, off);
          nextStartingOffsets.put(col, basedOnStartOffset);
        }
    );

    nextStartingOffsets.forEach(
        (col, off) -> nextMaxOffsets.put(col, lastPartition.generateNextPartitionOffset(col, off))
    );


    final TableRuntimeContext nextPartition = new TableRuntimeContext(
        lastPartition.sourceTableContext,
        lastPartition.usingNonIncrementalLoad,
        lastPartition.partitioned,
        newPartitionSequence,
        nextStartingOffsets,
        nextMaxOffsets
    );

    return nextPartition;
  }

  public String generateNextPartitionOffset(String column, String offset) {
    return TableContextUtil.generateNextPartitionOffset(
        sourceTableContext,
        column,
        offset
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableRuntimeContext that = (TableRuntimeContext) o;
    return
        partitioned == that.partitioned &&
        usingNonIncrementalLoad == that.usingNonIncrementalLoad &&
        partitionSequence == that.partitionSequence &&
        Objects.equals(
            sourceTableContext,
            that.sourceTableContext
        );
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceTableContext, partitioned, partitionSequence, usingNonIncrementalLoad);
  }

  public String getDescription() {
    return getOffsetKey();
  }

  public String getShortDescription() {
    return getQualifiedName() + ";" + getPartitionSequence();
  }

  public String getQualifiedName() {
    return sourceTableContext.getQualifiedName();
  }

  public void recordColumnOffset(String column, String offset) {
    recordColumnOffsets(Collections.singletonMap(column, offset));
  }

  public void recordColumnOffsets(Map<String, String> nextOffsets) {
    anyOffsetsRecorded = true;

    if (firstRecordedOffsets == null) {
      firstRecordedOffsets = new HashMap<>(nextOffsets);
    } else if (!firstRecordedOffsets.equals(nextOffsets)) {
      firstRecordedOffsetsPassed = true;
    }

    if (firstRecordedOffsetsPassed && startingPartitionOffsets.isEmpty()) {
      for (Map.Entry<String, String> entry : firstRecordedOffsets.entrySet()) {
        final String column = entry.getKey();
        final String offset = entry.getValue();
        if (!startingPartitionOffsets.containsKey(column)) {
          startingPartitionOffsets.put(column, offset);
          if (partitioned) {
            final String max = generateNextPartitionOffset(column, offset);
            maxPartitionOffsets.put(column, max);
          }
        }
      }
    }
  }

  public boolean isAnyOffsetsRecorded() {
    return anyOffsetsRecorded;
  }

  public boolean isMarkedNoMoreData() {
    return markedNoMoreData;
  }

  public void setMarkedNoMoreData(boolean markedNoMoreData) {
    this.markedNoMoreData = markedNoMoreData;
  }

  public boolean isResultSetProduced() {
    return resultSetProduced;
  }

  public void setResultSetProduced(boolean resultSetProduced) {
    this.resultSetProduced = resultSetProduced;
  }

  public boolean isFirstRecordedOffsetsPassed() {
    return firstRecordedOffsetsPassed;
  }
}
