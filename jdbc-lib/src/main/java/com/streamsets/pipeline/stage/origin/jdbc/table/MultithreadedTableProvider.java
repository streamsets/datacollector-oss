/**
 * Copyright 2017 StreamSets Inc.
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

import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Helper class for maintaining and organizing workable tables to threads
 */
public final class MultithreadedTableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MultithreadedTableProvider.class);

  private final Map<String, TableContext> tableContextMap;
  private final ArrayBlockingQueue<String> sharedAvailableTablesQueue;
  private final Set<String> tablesWithNoMoreData;
  private final Map<Integer, Integer> threadNumToMaxTableSlots;

  private final ThreadLocal<Deque<String>> ownedTablesQueue = ThreadLocal.withInitial(LinkedList::new);

  private volatile boolean isNoMoreDataEventGeneratedAlready = false;

  public MultithreadedTableProvider(
      Map<String, TableContext> tableContextMap,
      Queue<String> sortedTableOrder,
      Map<Integer, Integer> threadNumToMaxTableSlots
  ) {
    this.tableContextMap = new ConcurrentHashMap<>(tableContextMap);
    this.sharedAvailableTablesQueue =
        new ArrayBlockingQueue<>(tableContextMap.size(), true); //Using fair access policy
    sortedTableOrder.forEach(sharedAvailableTablesQueue::offer);
    this.tablesWithNoMoreData = Sets.newConcurrentHashSet();
    this.threadNumToMaxTableSlots = threadNumToMaxTableSlots;
  }

  private Deque<String> getOwnedTablesQueue() {
    return ownedTablesQueue.get();
  }

  private String getCurrentThreadName() {
    return Thread.currentThread().getName();
  }

  private void offerToOwnedTablesQueue(String acquiredTableName) {
    Optional.ofNullable(acquiredTableName).ifPresent(
        tableName -> {
          getOwnedTablesQueue().offerLast(tableName);
          LOG.trace("Thread '{}' has acquired table '{}'", getCurrentThreadName(), tableName);
        }
    );
  }

  /**
   * Basically acquires more tables for the current thread to work on.
   * The maximum a thread can hold is upper bounded to the
   * value the thread number was allocated in {@link #threadNumToMaxTableSlots}
   * If there are no tables currently owned make a blocking call to {@link #sharedAvailableTablesQueue}
   * else simply poll {@link #sharedAvailableTablesQueue} and it to the {@link #ownedTablesQueue}
   */
  private void acquireTableAsNeeded(int threadNumber) throws InterruptedException {
    int upperBoundOnTablesToAcquire = threadNumToMaxTableSlots.get(threadNumber);
    if (getOwnedTablesQueue().isEmpty()) {
      offerToOwnedTablesQueue(sharedAvailableTablesQueue.take());
    }
    String acquiredTableName;
    do {
      acquiredTableName = sharedAvailableTablesQueue.poll();
      offerToOwnedTablesQueue(acquiredTableName);
    } while (acquiredTableName != null && getOwnedTablesQueue().size()< upperBoundOnTablesToAcquire);
    //If we keep acquiring we will starve other threads, maxQueueSize is an upper bound
    //for number of tables we can acquire
  }

  /**
   * Return the next table to work on for the current thread (Will not return null)
   * Deque the current element from head of the queue and put it back at the tail to queue.
   */
  public TableContext nextTable(int threadNumber) throws SQLException, ExecutionException, StageException, InterruptedException {
    acquireTableAsNeeded(threadNumber);
    String tableName = getOwnedTablesQueue().pollFirst();
    LOG.trace("Thread '{}' has been assigned table '{}' to work on ", getCurrentThreadName(), tableName);
    //enqueue at the last because we just scheduled this table
    offerToOwnedTablesQueue(tableName);
    return tableContextMap.get(tableName);
  }

  /**
   * Release the current table worked on if don't have more rows to read or if
   * number of batches from the result set hit the upper bound based on configuration
   *
   * After releasing from the {@link #ownedTablesQueue} add it to the {@link #sharedAvailableTablesQueue}
   */
  public void releaseOwnedTable(TableContext tableContext) {
    String tableName = tableContext.getQualifiedName();
    LOG.trace("Thread '{}' has released ownership for table '{}'", getCurrentThreadName(),  tableName);

    //Remove the last element (because we put the current processing element at the tail of dequeue)
    String removedTableName = getOwnedTablesQueue().pollLast();
    Utils.checkState(
        tableName.equals(removedTableName),
        Utils.format(
            "Expected table to be remove '{}', Found '{}' at the last of the queue",
            tableName,
            removedTableName
        )
    );
    sharedAvailableTablesQueue.offer(tableName);
  }

  /**
   * Each {@link TableJdbcRunnable} worker thread can call this api to update
   * if there is data/no more data on the current table
   */
  public void reportDataOrNoMoreData(TableContext tableContext, boolean noMoreData) {
    if (noMoreData) {
      tablesWithNoMoreData.add(tableContext.getQualifiedName());
    } else {
      //When we see a table with data, we mark isNoMoreDataEventGeneratedAlready to false
      //so we can generate event again if we don't see data from all tables.
      isNoMoreDataEventGeneratedAlready = false;
      tablesWithNoMoreData.remove(tableContext.getQualifiedName());
    }
    LOG.trace("Number of Tables With No More Data {}", tablesWithNoMoreData.size());
  }

  /**
   * Used by the main thread {@link TableJdbcSource} to check whether all
   * tables have marked no more data
   * Generate event only if we haven't generate no more data event already
   * or we have seen a table with records after we generated an event before
   */
  public boolean shouldGenerateNoMoreDataEvent() {
    boolean noMoreData = (!isNoMoreDataEventGeneratedAlready && tablesWithNoMoreData.size() == tableContextMap.size());
    if (noMoreData) {
      tablesWithNoMoreData.clear();
      isNoMoreDataEventGeneratedAlready = true;
    }
    return noMoreData;
  }
}
