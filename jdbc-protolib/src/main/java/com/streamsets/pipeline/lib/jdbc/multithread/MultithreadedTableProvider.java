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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class for maintaining and organizing workable tables to threads
 */
public class MultithreadedTableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MultithreadedTableProvider.class);

  private Map<String, TableContext> tableContextMap;

  private final Multimap<String, TableContext> remainingSchemasToTableContexts = HashMultimap.create();
  private final Multimap<String, TableContext> completedSchemasToTableContexts = HashMultimap.create();
  private final LinkedList<TableRuntimeContext> sharedAvailableTablesList;
  private final Set<TableContext> tablesWithNoMoreData;
  private Map<Integer, Integer> threadNumToMaxTableSlots;
  private final int numThreads;
  private final BatchTableStrategy batchTableStrategy;
  private final TableMaxOffsetValueUpdater tableMaxOffsetValueUpdater;

  private Queue<String> sortedTableOrder;

  private final ThreadLocal<Deque<TableRuntimeContext>> ownedTablesQueue = ThreadLocal.withInitial(LinkedList::new);
  private final ThreadLocal<TableRuntimeContext> lastOwnedPartition = new ThreadLocal<>();
  private final ConcurrentMap<TableContext, Integer> maxPartitionWithDataPerTable = Maps.newConcurrentMap();

  private final SortedSetMultimap<TableContext, TableRuntimeContext> activeRuntimeContexts = TableRuntimeContext.buildSortedPartitionMap();
  private final Object partitionStateLock = activeRuntimeContexts;

  private final Set<TableRuntimeContext> removedPartitions = Sets.newConcurrentHashSet();

  private volatile boolean isNoMoreDataEventGeneratedAlready = false;

  public MultithreadedTableProvider(
      Map<String, TableContext> tableContextMap,
      Queue<String> sortedTableOrder,
      Map<Integer, Integer> threadNumToMaxTableSlots,
      int numThreads,
      BatchTableStrategy batchTableStrategy,
      TableMaxOffsetValueUpdater tableMaxOffsetValueUpdater
  ) {
    this.tableContextMap = new ConcurrentHashMap<>(tableContextMap);
    initializeRemainingSchemasToTableContexts();
    this.numThreads = numThreads;
    this.batchTableStrategy = batchTableStrategy;
    this.tableMaxOffsetValueUpdater = tableMaxOffsetValueUpdater;

    final Map<String, Integer> tableNameToOrder = new HashMap<>();
    int order = 1;
    for (String tableName : sortedTableOrder) {
      tableNameToOrder.put(tableName, order++);
    }

    sharedAvailableTablesList = new LinkedList<>();

    this.sortedTableOrder = new ArrayDeque<>(sortedTableOrder);

    // always construct initial values for partition queue based on table contexts
    // if stored offsets come into play, those will be handled by a subsequent invocation
    generateInitialPartitionsInSharedQueue(false, null, null);

    this.tablesWithNoMoreData = Sets.newConcurrentHashSet();
    this.threadNumToMaxTableSlots = threadNumToMaxTableSlots;
  }

  private void initializeRemainingSchemasToTableContexts() {
    for (final TableContext tableContext : this.tableContextMap.values()) {
      remainingSchemasToTableContexts.put(tableContext.getSchema(), tableContext);
    }
    completedSchemasToTableContexts.clear();
  }

  public void setTableContextMap(Map<String, TableContext> tableContextMap,
      Queue<String> sortedTableOrder) {
    if (!tableContextMap.equals(this.tableContextMap)) {
      this.tableContextMap = new ConcurrentHashMap<>(tableContextMap);

      final Map<String, Integer> tableNameToOrder = new HashMap<>();
      int order = 1;
      for (String tableName : sortedTableOrder) {
        tableNameToOrder.put(tableName, order++);
      }

      this.sortedTableOrder = new ArrayDeque<>(sortedTableOrder);

      // always construct initial values for partition queue based on table contexts
      // if stored offsets come into play, those will be handled by a subsequent invocation
      generateInitialPartitionsInSharedQueue(false, null, null);
    }
  }

  public Set<String> initializeFromV1Offsets(Map<String, String> offsets) throws StageException {
    // v1 offsets map qualified table names to offset column positions
    LOG.info("Upgrading offsets from v1 to v2; logging current offsets now");
    offsets.forEach((t, v) -> LOG.info("{} -> {}", t, v));

    final Set<String> offsetKeysToRemove = new HashSet<>();
    SortedSetMultimap<TableContext, TableRuntimeContext> v1Offsets = TableRuntimeContext.initializeAndUpgradeFromV1Offsets(
        tableContextMap,
        offsets,
        offsetKeysToRemove
    );
    generateInitialPartitionsInSharedQueue(true, v1Offsets, null);

    initializeMaxPartitionWithDataPerTable(offsets);
    return offsetKeysToRemove;
  }

  public void initializeFromV2Offsets(
      Map<String, String> offsets,
      Map<String, String> newCommitOffsets
  ) throws StageException {
    final Set<TableContext> excludeTables = new HashSet<>();
    SortedSetMultimap<TableContext, TableRuntimeContext> v2Offsets = TableRuntimeContext.buildPartitionsFromStoredV2Offsets(
        tableContextMap,
        offsets,
        excludeTables,
        newCommitOffsets
    );
    handlePartitioningTurnedOffOrOn(v2Offsets);
    generateInitialPartitionsInSharedQueue(true, v2Offsets, excludeTables);
    initializeMaxPartitionWithDataPerTable(newCommitOffsets);
  }

  /**
   * Checks whether any tables have had partitioning turned off or not, and updates the partition map appropriately
   *
   * @param reconstructedPartitions the reconstructed partitions (may be modified)
   */
  private void handlePartitioningTurnedOffOrOn(
      SortedSetMultimap<TableContext, TableRuntimeContext> reconstructedPartitions
  ) {

    for (TableContext tableContext : reconstructedPartitions.keySet()) {
      final SortedSet<TableRuntimeContext> partitions = reconstructedPartitions.get(tableContext);
      final TableRuntimeContext lastPartition = partitions.last();
      final TableContext sourceTableContext = lastPartition.getSourceTableContext();
      Utils.checkState(
          sourceTableContext.equals(tableContext),
          String.format(
              "Source table context for %s should match TableContext map key of %s",
              lastPartition.getDescription(),
              tableContext.getQualifiedName()
          )
      );

      final boolean partitioningTurnedOff = lastPartition.isPartitioned()
          && sourceTableContext.getPartitioningMode() == PartitioningMode.DISABLED;
      final boolean partitioningTurnedOn = !lastPartition.isPartitioned()
          && sourceTableContext.isPartitionable()
          && sourceTableContext.getPartitioningMode() != PartitioningMode.DISABLED;

      if (!partitioningTurnedOff && !partitioningTurnedOn) {
        continue;
      }

      final Map<String, String> nextStartingOffsets = new HashMap<>();
      final Map<String, String> nextMaxOffsets = new HashMap<>();

      final int newPartitionSequence = lastPartition.getPartitionSequence() > 0 ? lastPartition.getPartitionSequence() + 1 : 1;
      if (partitioningTurnedOff) {
        LOG.info(
            "Table {} has switched from partitioned to non-partitioned; partition sequence {} will be the last (with" +
                " no max offsets)",
            sourceTableContext.getQualifiedName(),
            newPartitionSequence
        );

        lastPartition.getPartitionOffsetStart().forEach(
            (col, off) -> {
              String basedOnStartOffset = lastPartition.generateNextPartitionOffset(col, off);
              nextStartingOffsets.put(col, basedOnStartOffset);
            }
        );

      } else if (partitioningTurnedOn) {

        lastPartition.getPartitionOffsetStart().forEach(
            (col, off) -> {
              String basedOnStoredOffset = lastPartition.getInitialStoredOffsets().get(col);
              nextStartingOffsets.put(col, basedOnStoredOffset);
            }
        );

        nextStartingOffsets.forEach(
            (col, off) -> nextMaxOffsets.put(col, lastPartition.generateNextPartitionOffset(col, off))
        );

        if (!reconstructedPartitions.remove(sourceTableContext, lastPartition)) {
          throw new IllegalStateException(String.format(
              "Failed to remove partition %s for table %s in switching partitioning from off to on",
              lastPartition.getDescription(),
              sourceTableContext.getQualifiedName()
          ));
        }

        LOG.info(
            "Table {} has switched from non-partitioned to partitioned; using last stored offsets as the starting" +
                " offsets for the new partition {}",
            sourceTableContext.getQualifiedName(),
            newPartitionSequence
        );
      }

      final TableRuntimeContext nextPartition = new TableRuntimeContext(
          sourceTableContext,
          lastPartition.isUsingNonIncrementalLoad(),
          (lastPartition.isPartitioned() && !partitioningTurnedOff) || partitioningTurnedOn,
          newPartitionSequence,
          nextStartingOffsets,
          nextMaxOffsets
      );

      reconstructedPartitions.put(sourceTableContext, nextPartition);
    }
  }

  @VisibleForTesting
  void generateInitialPartitionsInSharedQueue(
      boolean fromStoredOffsets,
      Multimap<TableContext, TableRuntimeContext> reconstructedPartitions,
      Set<TableContext> excludeTables
  ) {
    sharedAvailableTablesList.clear();
    activeRuntimeContexts.clear();
    for (String qualifiedTableName : sortedTableOrder) {
      //create the initial partition for each table
      final TableContext tableContext = tableContextMap.get(qualifiedTableName);

      if (excludeTables != null && excludeTables.contains(tableContext)) {
        LOG.debug("Not adding table {} to table provider since it was excluded", qualifiedTableName);
        // Since the table is ignored, we have to set the no-more-data like events as if the table was already transferred
        tablesWithNoMoreData.add(tableContext);
        remainingSchemasToTableContexts.remove(tableContext.getSchema(), tableContext);
        completedSchemasToTableContexts.put(tableContext.getSchema(), tableContext);
        continue;
      }
      Collection<TableRuntimeContext> partitions = null;
      if (fromStoredOffsets) {
        partitions = reconstructedPartitions.get(tableContext);
      }
      if (partitions == null  || partitions.isEmpty()) {
        partitions = Collections.singletonList(TableRuntimeContext.createInitialPartition(tableContext));
      }

      partitions.forEach(sharedAvailableTablesList::add);
      activeRuntimeContexts.putAll(tableContext, partitions);
    }
  }

  @VisibleForTesting
  void initializeMaxPartitionWithDataPerTable(Map<String, String> offsets) {

    Map<TableContext, Integer> maxWithData = new HashMap<>();
    for (TableContext table : activeRuntimeContexts.keySet()) {
      final SortedSet<TableRuntimeContext> partitions = activeRuntimeContexts.get(table);
      TableRuntimeContext firstPartition = partitions.first();
      // as a baseline, the partition sequence one lower than the minimum reconstructed partition has had data
      // since otherwise, it would still be part of the stored data
      if (firstPartition.isPartitioned()) {
        maxWithData.put(table, firstPartition.getPartitionSequence() - 1);
      }
      for (TableRuntimeContext partition : partitions) {
        if (offsets.containsKey(partition.getOffsetKey())) {

          final Map<String, String> storedOffsets = OffsetQueryUtil.getColumnsToOffsetMapFromOffsetFormat(
              offsets.get(partition.getOffsetKey())
          );

          final Map<String, String> startOffsets = partition.getPartitionOffsetStart();
          for (Map.Entry<String, String> storedOffsetEntry : storedOffsets.entrySet()) {
            String offsetCol = storedOffsetEntry.getKey();
            String storedOffsetVal = storedOffsetEntry.getValue();
            if (startOffsets.get(offsetCol) != null && !startOffsets.get(offsetCol).equals(storedOffsetVal)) {
              // if the stored offset value is not equal to the starting offset value, it must necessarily be greater
              // (since records are processed in increasing order w.r.t. the offset column)
              // therefore, we know that progress has been made in this partition in a previous run

              if (maxWithData.containsKey(table)) {
                int partitionSequence = partition.getPartitionSequence();
                if (maxWithData.get(table) < partitionSequence) {
                  maxWithData.put(table, partitionSequence);
                }
              }
            }
          }
        }
      }
    }

    maxPartitionWithDataPerTable.putAll(maxWithData);
  }

  @VisibleForTesting
  Deque<TableRuntimeContext> getOwnedTablesQueue() {
    return ownedTablesQueue.get();
  }

  @VisibleForTesting
  TableRuntimeContext getLastOwnedPartition() {
    return lastOwnedPartition.get();
  }

  private String getCurrentThreadName() {
    return Thread.currentThread().getName();
  }

  @VisibleForTesting
  Multimap<String, TableContext> getRemainingSchemasToTableContexts() {
    return remainingSchemasToTableContexts;
  }

  public Multimap<String, TableContext> getCompletedSchemasToTableContexts() {
    return completedSchemasToTableContexts;
  }

  @VisibleForTesting
  void offerToOwnedTablesQueue(TableRuntimeContext acquiredTableName, int threadNumber) {
    getOwnedTablesQueue().offerLast(acquiredTableName);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Thread '{}' has acquired table '{}'", getCurrentThreadName(), acquiredTableName.getDescription());
    }
  }

  /**
   * Basically acquires more tables for the current thread to work on.
   * The maximum a thread can hold is upper bounded to the
   * value the thread number was allocated in {@link #threadNumToMaxTableSlots}
   * If there are no tables currently owned make a blocking call to {@link #sharedAvailableTablesList}
   * else simply poll {@link #sharedAvailableTablesList} and it to the {@link #ownedTablesQueue}
   */
  @VisibleForTesting
  void acquireTableAsNeeded(int threadNumber) throws InterruptedException {
    if (!getOwnedTablesQueue().isEmpty() && batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      circleOffAllOwnedTablePartitionsToEnd();

      if (!sharedAvailableTablesList.isEmpty()
          && threadNumToMaxTableSlots.get(threadNumber) > getOwnedTablesQueue().size()) {
        acquirePartition();
      }
    } else {
      if (getOwnedTablesQueue().isEmpty()) {
        TableRuntimeContext head = sharedAvailableTablesList.pollFirst();
        if (head != null) {
          offerToOwnedTablesQueue(head, threadNumber);
        }
      }
    }

    partitionFirstSharedQueueItemIfNeeded();
  }

  /**
   * Cycle off all partitions from the same table to the end of the queue.
   */
  private void circleOffAllOwnedTablePartitionsToEnd() {
    TableRuntimeContext lastOwnedPartition = getLastOwnedPartition();
    TableContext lastOwnedTable = lastOwnedPartition.getSourceTableContext();
    TableRuntimeContext first = sharedAvailableTablesList.peekFirst();
    TableRuntimeContext current = first;
    while (current != null && current.getSourceTableContext().equals(lastOwnedTable)
        && !current.equals(lastOwnedPartition)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Moving partition {} to end of shared queue to comply with BatchTableStrategy of {}",
            current.getDescription(),
            batchTableStrategy.getLabel()
        );
      }

      // move item from head to tail of list
      TableRuntimeContext toMove = sharedAvailableTablesList.pollFirst();
      sharedAvailableTablesList.add(toMove);
      // Get the new head of the queue
      current = sharedAvailableTablesList.peekFirst();
      if (current == first) {
        // All partitions belong to the last owned table,
        // so we made a full circle and now we have to go out.
        break;
      }
    }
  }

  /**
   * We will search for the first not empty table to own it.
   * If all tables are empty and we own another table we will not take anything:
   * let other threads (if any) try checking if there are new data.
   */
  private void acquirePartition() {
    TableRuntimeContext lastOwnedPartition = getLastOwnedPartition();

    boolean added = false;
    TableRuntimeContext last = sharedAvailableTablesList.peekLast();
    TableRuntimeContext current = sharedAvailableTablesList.pollFirst();
    while(true) {
      TableContext candidate = current.getSourceTableContext();
      if (!added && !tablesWithNoMoreData.contains(current.getSourceTableContext())
          // We will take only partitions of other tables (to implement the SWITCH_TABLES strategy).
          // It doesn't make sens to take another partition of the same table
          // If we already work on a table, let's first finish with that partition,
          // and let's alllow other threads to take a partition of this table to improve performance.
          && getOwnedTablesQueue().stream().map(ot -> ot.getSourceTableContext()).noneMatch(ot -> ot == candidate)
      ) {
        // There is a not empty table
        if (lastOwnedPartition == getOwnedTablesQueue().peekLast()) {
          getOwnedTablesQueue().offerFirst(current);
        } else {
          getOwnedTablesQueue().offerLast(current);
        }
        added = true;
      } else {
        sharedAvailableTablesList.offerLast(current);
      }
      if (current == last) { // We walked throw all avaialble partitions, we need to go out
        break;
      }
      current = sharedAvailableTablesList.pollFirst();
    }
  }

  /**
   * <p>Examines the first item ("head") im the shared partition queue, and adds a new partition if appropriate</p>
   * <p>A new partition will be created if the number of partitions for the head item's table is still less
   * than the maximum, and that table itself is partitionable</p>
   */
  @VisibleForTesting
  void partitionFirstSharedQueueItemIfNeeded() {
    final TableRuntimeContext headPartition = getOwnedTablesQueue().peek();
    if (headPartition != null) {
      synchronized (partitionStateLock) {
        keepPartitioningIfNeeded(headPartition);
      }
    } else if (LOG.isTraceEnabled()) {
      LOG.trace("No item at head of shared partition queue");
    }
  }

  @VisibleForTesting
  void keepPartitioningIfNeeded(TableRuntimeContext partition) {
    TableRuntimeContext current = partition;
    while (current != null && isNewPartitionAllowed(current)) {
      TableRuntimeContext newPartition = createNextPartition(current);
      if (newPartition != null) {
        LOG.info("Adding new partition to shared queue: {}", newPartition.getDescription());
        activeRuntimeContexts.put(newPartition.getSourceTableContext(), newPartition);
        if (!sharedAvailableTablesList.add(newPartition)) {
          return;
        }
        current = newPartition;
      } else {
        current = null;
      }
    }
  }

  @VisibleForTesting
  boolean isNewPartitionAllowed(TableRuntimeContext partition) {
    final TableContext tableContext = partition.getSourceTableContext();
    if (!partition.isPartitioned() &&
        (tableContext.getPartitioningMode() == PartitioningMode.DISABLED || !tableContext.isPartitionable())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot create new partition for ({}) because it is not partitionable, and the underlying table is" +
                "not partitionable, or it has been disabled",
            partition.getDescription()
        );
      }
      return false;
    }
    if (tablesWithNoMoreData.contains(tableContext)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot create new partition for ({}) because the table has already been marked exhausted in this iteration",
            partition.getDescription()
        );
      }
      return false;
    }

    final boolean maxOffsetValuesPassed = TableContextUtil.allOffsetsBeyondMaxValues(
        tableContext,
        partition.getPartitionOffsetStart()
    );

    final int maxPartitionWithData = getMaxPartitionWithData(tableContext);
    if (maxOffsetValuesPassed
        && partition.getPartitionSequence() - maxPartitionWithData > maxNumActivePartitions(tableContext)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot create new partition for ({}) because there has been no data seen since partition {}",
            partition.getDescription(),
            maxPartitionWithData
        );
      }
      return false;
    }

    // check whether this particular table already has the maximum number of allowed partitions
    final SortedSet<TableRuntimeContext> runtimeContexts = activeRuntimeContexts.get(tableContext);
    final int maxNumActivePartitions = maxNumActivePartitions(tableContext);
    if (maxOffsetValuesPassed && runtimeContexts.size() >= maxNumActivePartitions) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot create new partition for ({}) because the table has already reached the maximum allowed number of" +
                " active partitions ({})",
            partition.getDescription(),
            maxNumActivePartitions
        );
      }
      return false;
    }

    if (runtimeContexts.size() > 0 && !runtimeContexts.last().equals(partition)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Can only create new partition for ({}) if it is the last currently active partition for the table",
            partition.getDescription()
        );
      }
      return false;
    }

    return true;
  }

  @VisibleForTesting
  int getMaxPartitionWithData(TableContext tableContext) {
    final Integer maxPartitionWithDataObj = maxPartitionWithDataPerTable.get(tableContext);
    return maxPartitionWithDataObj != null ? maxPartitionWithDataObj : 0;
  }

  @VisibleForTesting
  int maxNumActivePartitions(TableContext tableContext) {
    if (tableContext.getMaxNumActivePartitions() > 0) {
      return tableContext.getMaxNumActivePartitions();
    } else {
      // numThreads * 2 gives enough of a cushion such that enough new partitions can be created so that threads
      // always have something to work on, while also maintaining previous partitions that may not yet have
      // finished and been removed from the active ontext
      return numThreads * 2;
    }
  }

  @VisibleForTesting
  boolean removePartitionIfNeeded(TableRuntimeContext partition) {
    final TableContext sourceTableContext = partition.getSourceTableContext();
    synchronized (partitionStateLock) {
      boolean tableExhausted = false;

      final SortedSet<TableRuntimeContext> activeContexts = activeRuntimeContexts.get(sourceTableContext);

      final Iterator<TableRuntimeContext> activeContextIter = activeContexts.iterator();
      int numActivePartitions = 0;
      int positionsFromEnd = activeContexts.size();
      while (activeContextIter.hasNext()) {
        final TableRuntimeContext thisPartition = activeContextIter.next();

        if (thisPartition.equals(partition)) {
          final int maxPartitionWithData = getMaxPartitionWithData(partition.getSourceTableContext());

          // update max offset values for table, in case new rows have been added since initialization
          tableMaxOffsetValueUpdater.updateMaxOffsetsForTable(sourceTableContext);

          final boolean lastPartition =
              // no currently active partitions for the table
              numActivePartitions == 0
              // and the number of partitions since we last saw data
              && partition.getPartitionSequence() - maxPartitionWithData
              // is greater than or equal to the max number of active partitions minus 1
              >= (maxNumActivePartitions(sourceTableContext) - 1)
              && TableContextUtil.allOffsetsBeyondMaxValues(
                  sourceTableContext,
                  partition.getPartitionOffsetStart()
              )
              ;
          if (!activeContextIter.hasNext() && thisPartition.isMarkedNoMoreData()
              && (!partition.isPartitioned() || lastPartition)) {
            // this is the last partition, and was already marked no more data once
            // now, it's being marked no more data again, so we can safely assume that the table is now exhausted
            tableExhausted = true;
          }
          break;
        } else if (thisPartition.isMarkedNoMoreData() && activeContextIter.hasNext()) {
          // this partition has already been marked as no more data once, so it can be removed now
          // but only if there is at least one more after it, since we want to keep at least one for every table

          if (positionsFromEnd > maxNumActivePartitions(sourceTableContext)
              || thisPartition.getPartitionSequence() < getMaxPartitionWithData(thisPartition.getSourceTableContext())) {

            activeContextIter.remove();
            removedPartitions.add(thisPartition);
            if (!sharedAvailableTablesList.remove(thisPartition)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Failed to remove partition {} from sharedAvailableTablesList; it may be owned by another thread",
                    thisPartition.getDescription()
                );
              }
            }
          }
          // else this partition will simply NOT be re-added to the shared queue in the releaseOwnedTable method
        } else {
          numActivePartitions++;
        }
        positionsFromEnd--;
      }
      return tableExhausted;
    }
  }

  /**
   * Return the next table to work on for the current thread (Will not return null)
   * Deque the current element from head of the queue and put it back at the tail to queue.
   */
  public TableRuntimeContext nextTable(int threadNumber) throws InterruptedException {
    synchronized (partitionStateLock) {
      acquireTableAsNeeded(threadNumber);

      final TableRuntimeContext partition = getOwnedTablesQueue().pollFirst();
      if (partition == null) {
        lastOwnedPartition.remove();
      } else {
        lastOwnedPartition.set(partition);
        offerToOwnedTablesQueue(partition, threadNumber);
      }
      return partition;
    }
  }

  @VisibleForTesting
  TableRuntimeContext createNextPartition(TableRuntimeContext lastContext) {
    TableRuntimeContext runtimeContext = TableRuntimeContext.createNextPartition(lastContext);
    TableContext tableContext = lastContext.getSourceTableContext();

    if (runtimeContext != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Creating next partition (number {}) for thread '{}' to work on table '{}'",
            runtimeContext.getPartitionSequence(),
            getCurrentThreadName(),
            tableContext.getQualifiedName()
        );
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Offsets for table '{}' partition {}: start=({}), max=({})",
            tableContext.getQualifiedName(),
            runtimeContext.getPartitionSequence(),
            runtimeContext.getPartitionOffsetStart(),
            runtimeContext.getPartitionOffsetEnd()
        );
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Could not create next partition (after number {}) for thread '{}' to work on for table '{}' because"
                + " offsets ({}) have not yet been captured for every offset column ({})",
            lastContext.getPartitionSequence(),
            getCurrentThreadName(),
            tableContext.getQualifiedName(),
            lastContext.getPartitionOffsetStart(),
            tableContext.getOffsetColumns()
        );
      }
    }
    return runtimeContext;
  }

  @VisibleForTesting
  void releaseOwnedTable(TableRuntimeContext tableRuntimeContext, int threadNumber) {
    final TableContext sourceContext = tableRuntimeContext.getSourceTableContext();

    String tableName = sourceContext.getQualifiedName();
    LOG.trace(
        "Thread '{}' has released ownership for partition '{}'",
        getCurrentThreadName(),
        tableRuntimeContext
    );

    //Remove the last element (because we put the current processing element at the tail of dequeue)
    TableRuntimeContext removedPartition = getOwnedTablesQueue().pollLast();
    Utils.checkState(
        tableRuntimeContext.equals(removedPartition),
        Utils.format(
            "Expected table to be remove '{}', Found '{}' at the last of the queue",
            tableName,
            removedPartition.getDescription()
        )
    );
    synchronized (partitionStateLock) {
      boolean containsActiveEntry = activeRuntimeContexts.containsEntry(sourceContext, removedPartition);
      if (containsActiveEntry || sharedAvailableTablesList.isEmpty()) {
        if (tableRuntimeContext.isUsingNonIncrementalLoad()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Not re-adding table {} because it is non-incremental", removedPartition.getDescription());
          }
          return;
        }
        sharedAvailableTablesList.add(removedPartition);
        if (!containsActiveEntry) {
          activeRuntimeContexts.put(sourceContext, removedPartition);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Not adding partition '{}' back to the shared queue because it was already removed as an active" +
                  " context, and the queue is not empty",
              removedPartition.getDescription()
          );
        }
      }
    }
  }

  void reportDataOrNoMoreData(
      TableRuntimeContext tableRuntimeContext,
      int recordCount,
      int batchSize,
      boolean resultSetEndReached
  ) {
    reportDataOrNoMoreData(tableRuntimeContext, recordCount, batchSize, resultSetEndReached, null, null, null);
  }

  /**
   * Each {@link TableJdbcRunnable} worker thread can call this api to update
   * if there is data/no more data on the current table
   */
  public void reportDataOrNoMoreData(
      TableRuntimeContext tableRuntimeContext,
      int recordCount,
      int batchSize,
      boolean resultSetEndReached,
      AtomicBoolean tableFinished,
      AtomicBoolean schemaFinished,
      List<String> schemaFinishedTables
  ) {
    final TableContext sourceContext = tableRuntimeContext.getSourceTableContext();

    // When we see a table with data, we mark isNoMoreDataEventGeneratedAlready to false
    // so we can generate event again if we don't see data from all tables.
    if(recordCount > 0) {
      isNoMoreDataEventGeneratedAlready = false;
      tablesWithNoMoreData.remove(tableRuntimeContext.getSourceTableContext());
      remainingSchemasToTableContexts.put(sourceContext.getSchema(), sourceContext);
      completedSchemasToTableContexts.remove(sourceContext.getSchema(), sourceContext);
    }

    // we need to account for the activeRuntimeContexts here
    // if there are still other active contexts in process, then this should do "nothing"
    // if there are not other contexts, we need to figure out what the highest offset completed by the last batch was

    final boolean noMoreData = recordCount == 0 || resultSetEndReached;

    if (noMoreData) {
      tableRuntimeContext.setMarkedNoMoreData(true);
    }

    if (recordCount > 0) {
      maxPartitionWithDataPerTable.put(sourceContext, tableRuntimeContext.getPartitionSequence());
    }

    boolean tableExhausted = removePartitionIfNeeded(tableRuntimeContext);

    if (noMoreData) {
      if (tableExhausted) {
        synchronized (this) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Table {} exhausted",
                sourceContext.getQualifiedName()
            );
          }

          final boolean newlyFinished = tablesWithNoMoreData.add(sourceContext);

          if (newlyFinished && tableFinished != null) {
            tableFinished.set(true);
          }

          final boolean remainingSchemaChanged = remainingSchemasToTableContexts.remove(sourceContext.getSchema(), sourceContext);
          completedSchemasToTableContexts.put(sourceContext.getSchema(), sourceContext);

          if (remainingSchemaChanged && remainingSchemasToTableContexts.get(sourceContext.getSchema()).isEmpty() && schemaFinished != null) {
            schemaFinished.set(true);
            if (schemaFinishedTables != null) {
              completedSchemasToTableContexts.get(sourceContext.getSchema()).forEach(
                  t -> schemaFinishedTables.add(t.getTableName())
              );
            }
          }
        }
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Just released table {}; Number of Tables With No More Data {}",
          tableRuntimeContext.getDescription(),
          tablesWithNoMoreData.size()
      );
    }
  }

  /**
   * Used by the main thread {@link TableJdbcSource} to check whether all
   * tables have marked no more data
   * Generate event only if we haven't generate no more data event already
   * or we have seen a table with records after we generated an event before
   */
  public synchronized boolean shouldGenerateNoMoreDataEvent() {
    boolean noMoreData = (
        !isNoMoreDataEventGeneratedAlready &&
            tablesWithNoMoreData.size() == tableContextMap.size());
    if (noMoreData) {
      isNoMoreDataEventGeneratedAlready = true;
    }
    return noMoreData;
  }

  public List<TableRuntimeContext> getAndClearRemovedPartitions() {
    synchronized (partitionStateLock) {
      final LinkedList<TableRuntimeContext> returnPartitions = new LinkedList<>(removedPartitions);
      removedPartitions.clear();
      return returnPartitions;
    }
  }

  @VisibleForTesting
  public Map<String, TableContext> getTableContextMap() {
    return tableContextMap;
  }

  @VisibleForTesting
  LinkedList<TableRuntimeContext> getSharedAvailableTablesList() {
    return sharedAvailableTablesList;
  }

  @VisibleForTesting
  Set<TableContext> getTablesWithNoMoreData() {
    return tablesWithNoMoreData;
  }

  @VisibleForTesting
  Map<Integer, Integer> getThreadNumToMaxTableSlots() {
    return threadNumToMaxTableSlots;
  }

  @VisibleForTesting
  int getNumThreads() {
    return numThreads;
  }

  @VisibleForTesting
  public ConcurrentMap<TableContext, Integer> getMaxPartitionWithDataPerTable() {
    return maxPartitionWithDataPerTable;
  }

  @VisibleForTesting
  public SortedSetMultimap<TableContext, TableRuntimeContext> getActiveRuntimeContexts() {
    return activeRuntimeContexts;
  }

  @VisibleForTesting
  String getAllState() {
    final StringBuilder sb = new StringBuilder();
    sb.append("owned: ");
    sb.append(getOwnedQueueState());
    sb.append("\nshared: ");
    sb.append(getSharedQueueState());
    sb.append("\nactive: ");
    sb.append(getActiveContextsState());
    return sb.toString();
  }

  @VisibleForTesting
  String getOwnedQueueState() {
    final StringBuilder sb = new StringBuilder();
    for (TableRuntimeContext item : getOwnedTablesQueue()) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(item.getShortDescription());
    }
    return sb.toString();
  }

  @VisibleForTesting
  String getSharedQueueState() {
    final StringBuilder sb = new StringBuilder();
    for (TableRuntimeContext item : sharedAvailableTablesList) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(item.getShortDescription());
    }
    return sb.toString();
  }

  @VisibleForTesting
  String getActiveContextsState() {
    final StringBuilder sb = new StringBuilder();
    for (TableContext table : activeRuntimeContexts.keySet()) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append(table.getQualifiedName());
      sb.append(": [");
      boolean seen = false;
      for (TableRuntimeContext partition : activeRuntimeContexts.get(table)) {
        if (seen) {
          sb.append(",");
        }
        sb.append(partition.getPartitionSequence());
        if (partition.isMarkedNoMoreData()) {
          sb.append("*");
        }
        seen = true;
      }
      sb.append(": ]");
    }
    return sb.toString();
  }
}
