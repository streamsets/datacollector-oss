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
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource;
import org.apache.commons.collections.CollectionUtils;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Helper class for maintaining and organizing workable tables to threads
 */
public final class MultithreadedTableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MultithreadedTableProvider.class);

  /**
   * The factor by which the sum of the max number of active partitions of all tables is multiplied by to determine
   * the overall size of the shared partition queue.  It exists to ensure there is always enough capacity to offer
   * partitions to the queue as needed.
   */
  private static final int SHARED_QUEUE_SIZE_FUDGE_FACTOR = 2;

  private final Map<String, TableContext> tableContextMap;
  private final BlockingQueue<TableRuntimeContext> sharedAvailableTablesQueue;
  private final Set<TableContext> tablesWithNoMoreData;
  private final Map<Integer, Integer> threadNumToMaxTableSlots;
  private final int numThreads;
  private final BatchTableStrategy batchTableStrategy;

  private final Queue<String> sortedTableOrder;

  private final ThreadLocal<Deque<TableRuntimeContext>> ownedTablesQueue = ThreadLocal.withInitial(LinkedList::new);
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
      BatchTableStrategy batchTableStrategy
  ) {
    this.tableContextMap = new ConcurrentHashMap<>(tableContextMap);
    this.numThreads = numThreads;
    this.batchTableStrategy = batchTableStrategy;

    final Map<String, Integer> tableNameToOrder = new HashMap<>();
    int order = 1;
    for (String tableName : sortedTableOrder) {
      tableNameToOrder.put(tableName, order++);
    }

    int sharedPartitionQueueSize = 0;
    for (TableContext tableContext : tableContextMap.values()) {
      sharedPartitionQueueSize += maxNumActivePartitions(tableContext);
    }
    sharedAvailableTablesQueue = new ArrayBlockingQueue<TableRuntimeContext>(
        sharedPartitionQueueSize * SHARED_QUEUE_SIZE_FUDGE_FACTOR,
        true
    );

    this.sortedTableOrder = new ArrayDeque<>(sortedTableOrder);

    // always construct initial values for partition queue based on table contexts
    // if stored offsets come into play, those will be handled by a subsequent invocation
    generateInitialPartitionsInSharedQueue(false, null, null);

    this.tablesWithNoMoreData = Sets.newConcurrentHashSet();
    this.threadNumToMaxTableSlots = threadNumToMaxTableSlots;
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

        lastPartition.getStartingPartitionOffsets().forEach(
            (col, off) -> {
              String basedOnStartOffset = lastPartition.generateNextPartitionOffset(col, off);
              nextStartingOffsets.put(col, basedOnStartOffset);
            }
        );

      } else if (partitioningTurnedOn) {

        lastPartition.getStartingPartitionOffsets().forEach(
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
                " offsets for the new partition",
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
    sharedAvailableTablesQueue.clear();
    activeRuntimeContexts.clear();
    for (String qualifiedTableName : sortedTableOrder) {
      //create the initial partition for each table
      final TableContext tableContext = tableContextMap.get(qualifiedTableName);

      if (excludeTables != null && excludeTables.contains(tableContext)) {
        LOG.debug("Not adding table {} to table provider since it was excluded", qualifiedTableName);
        continue;
      }
      Collection<TableRuntimeContext> partitions = null;
      if (fromStoredOffsets) {
        partitions = reconstructedPartitions.get(tableContext);
      }
      if (CollectionUtils.isEmpty(partitions)) {
        partitions = Collections.singletonList(TableRuntimeContext.createInitialPartition(tableContext));
      }

      partitions.forEach(sharedAvailableTablesQueue::offer);
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

          final Map<String, String> startOffsets = partition.getStartingPartitionOffsets();
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

  private String getCurrentThreadName() {
    return Thread.currentThread().getName();
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
   * If there are no tables currently owned make a blocking call to {@link #sharedAvailableTablesQueue}
   * else simply poll {@link #sharedAvailableTablesQueue} and it to the {@link #ownedTablesQueue}
   */
  @VisibleForTesting
  void acquireTableAsNeeded(int threadNumber) throws InterruptedException {
    if (!getOwnedTablesQueue().isEmpty() && batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      final TableRuntimeContext lastOwnedPartition = getOwnedTablesQueue().pollLast();
      sharedAvailableTablesQueue.offer(lastOwnedPartition);

      TableContext lastOwnedTable = lastOwnedPartition.getSourceTableContext();
      // need to cycle off all partitions from the same table to the end of the queue
      TableRuntimeContext first = sharedAvailableTablesQueue.peek();
      while (first != null && first.getSourceTableContext().equals(lastOwnedTable)
          && !first.equals(lastOwnedPartition)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Moving partition {} to end of shared queue to comply with BatchTableStrategy of {}",
              first.getDescription(),
              batchTableStrategy.getLabel()
          );
        }
        TableRuntimeContext toMove = sharedAvailableTablesQueue.poll();
        sharedAvailableTablesQueue.offer(toMove);
        first = sharedAvailableTablesQueue.peek();
      }
    }

    if (getOwnedTablesQueue().isEmpty()) {
      TableRuntimeContext head = sharedAvailableTablesQueue.poll();
      if (head != null) {
        offerToOwnedTablesQueue(head, threadNumber);
      }
    }

    partitionFirstSharedQueueItemIfNeeded();
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
        if (!sharedAvailableTablesQueue.offer(newPartition)) {
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

    final int maxPartitionWithData = getMaxPartitionWithData(tableContext);
    if (partition.getPartitionSequence() - maxPartitionWithData > maxNumActivePartitions(tableContext)) {
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
    if (runtimeContexts.size() >= maxNumActivePartitions) {
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
      final TableContext sourceContext = sourceTableContext;

      final SortedSet<TableRuntimeContext> activeContexts = activeRuntimeContexts.get(sourceContext);

      final Iterator<TableRuntimeContext> activeContextIter = activeContexts.iterator();
      int numActivePartitions = 0;
      int positionsFromEnd = activeContexts.size();
      while (activeContextIter.hasNext()) {
        final TableRuntimeContext thisPartition = activeContextIter.next();

        if (thisPartition.equals(partition)) {
          final int maxPartitionWithData = getMaxPartitionWithData(partition.getSourceTableContext());
          final boolean lastPartition =
              // no currently active partitions for the table
              numActivePartitions == 0
              // and the number of partitions since we last saw data
              && partition.getPartitionSequence() - maxPartitionWithData
              // is greater than or equal to the max number of active partitions minus 1
              >= (maxNumActivePartitions(partition.getSourceTableContext()) - 1);
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

          if (positionsFromEnd > maxNumActivePartitions(partition.getSourceTableContext())
              || thisPartition.getPartitionSequence() < getMaxPartitionWithData(thisPartition.getSourceTableContext())) {

            activeContextIter.remove();
            removedPartitions.add(thisPartition);
            if (!sharedAvailableTablesQueue.remove(thisPartition)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Failed to remove partition {} from sharedAvailableTablesQueue; it may be owned by another thread",
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
      if (partition != null) {
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
            runtimeContext.getStartingPartitionOffsets(),
            runtimeContext.getMaxPartitionOffsets()
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
            lastContext.getStartingPartitionOffsets(),
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
        "Thread '{}' has released ownership for table '{}'",
        getCurrentThreadName(),
        tableRuntimeContext.getDescription()
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
      if (containsActiveEntry || sharedAvailableTablesQueue.isEmpty()) {
        if (tableRuntimeContext.isUsingNonIncrementalLoad()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Not re-adding table {} because it is non-incremental", removedPartition.getDescription());
          }
          return;
        }
        try {
          sharedAvailableTablesQueue.put(removedPartition);
        } catch (InterruptedException e) {
          LOG.error(
              "InterruptedException trying to put partition {} back on shared queue",
              removedPartition.getDescription(),
              e
          );
          Thread.currentThread().interrupt();
        }
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

  /**
   * Each {@link TableJdbcRunnable} worker thread can call this api to update
   * if there is data/no more data on the current table
   */
  public void reportDataOrNoMoreData(
      TableRuntimeContext tableRuntimeContext,
      int recordCount,
      int batchSize,
      boolean resultSetEndReached
  ) {

    final TableContext sourceContext = tableRuntimeContext.getSourceTableContext();

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
        tablesWithNoMoreData.add(tableRuntimeContext.getSourceTableContext());
      }
    } else {
      //When we see a table with data, we mark isNoMoreDataEventGeneratedAlready to false
      //so we can generate event again if we don't see data from all tables.
      isNoMoreDataEventGeneratedAlready = false;
      tablesWithNoMoreData.remove(tableRuntimeContext.getSourceTableContext());
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
  public boolean shouldGenerateNoMoreDataEvent() {
    boolean noMoreData = (
        !isNoMoreDataEventGeneratedAlready &&
            tablesWithNoMoreData.size() == tableContextMap.size());
    if (noMoreData) {
      tablesWithNoMoreData.clear();
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
  BlockingQueue<TableRuntimeContext> getSharedAvailableTablesQueue() {
    return sharedAvailableTablesQueue;
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
    for (TableRuntimeContext item : sharedAvailableTablesQueue) {
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
