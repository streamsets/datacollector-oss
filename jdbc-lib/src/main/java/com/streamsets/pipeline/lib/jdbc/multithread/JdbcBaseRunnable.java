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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.cache.JdbcTableReadContextInvalidationListener;
import com.streamsets.pipeline.lib.jdbc.multithread.cache.JdbcTableReadContextLoader;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcELEvalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class JdbcBaseRunnable implements Runnable, JdbcRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcBaseRunnable.class);
  protected static final String JDBC_NAMESPACE_HEADER = "jdbc.";
  private static final long ACQUIRE_TABLE_SLEEP_INTERVAL = 5L;

  static final String THREAD_NAME = "Thread Name";
  public static final String CURRENT_TABLE = "Current Table";
  static final String TABLES_OWNED_COUNT = "Tables Owned";
  static final String STATUS = "Status";
  public static final String TABLE_METRICS = "Table Metrics for Thread - ";
  public static final String TABLE_JDBC_THREAD_PREFIX = "Table Jdbc Runner - ";

  public static final String PARTITION_ATTRIBUTE = JDBC_NAMESPACE_HEADER + "partition";
  public static final String THREAD_NUMBER_ATTRIBUTE = JDBC_NAMESPACE_HEADER + "threadNumber";

  protected final PushSource.Context context;
  protected final Map<String, String> offsets;
  private final LoadingCache<TableRuntimeContext, TableReadContext> tableReadContextCache;
  private final MultithreadedTableProvider tableProvider;
  protected final int threadNumber;
  private final int batchSize;
  protected final TableJdbcConfigBean tableJdbcConfigBean;
  protected final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcELEvalContext tableJdbcELEvalContext;
  private final ConnectionManager connectionManager;
  protected final ErrorRecordHandler errorRecordHandler;
  private final Map<String, Object> gaugeMap;

  private TableRuntimeContext tableRuntimeContext;
  private long lastQueryIntervalTime;

  private int numSQLErrors = 0;
  private SQLException firstSqlException = null;

  private enum Status {
    QUERYING_TABLE,
    GENERATING_BATCH,
    BATCH_GENERATED,
    ;
  }

  public JdbcBaseRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, String> offsets,
      MultithreadedTableProvider tableProvider,
      ConnectionManager connectionManager,
      TableJdbcConfigBean tableJdbcConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CacheLoader<TableRuntimeContext, TableReadContext> tableCacheLoader
  ) {
    this.context = context;
    this.threadNumber = threadNumber;
    this.lastQueryIntervalTime = -1;
    this.batchSize = batchSize;
    this.tableJdbcELEvalContext = new TableJdbcELEvalContext(context, context.createELVars());
    this.offsets = offsets;
    this.tableProvider = tableProvider;
    this.tableJdbcConfigBean = tableJdbcConfigBean;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.connectionManager = connectionManager;
    this.tableReadContextCache = buildReadContextCache(tableCacheLoader);
    this.errorRecordHandler = new DefaultErrorRecordHandler(context, (ToErrorContext) context);
    this.numSQLErrors = 0;
    this.firstSqlException = null;

    // Metrics
    this.gaugeMap = context.createGauge(TABLE_METRICS + threadNumber).getValue();
  }

  public LoadingCache<TableRuntimeContext, TableReadContext> getTableReadContextCache() {
    return tableReadContextCache;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(TABLE_JDBC_THREAD_PREFIX + threadNumber);
    initGaugeIfNeeded();
    while (!context.isStopped()) {
      generateBatchAndCommitOffset(context.startBatch());
    }
  }

  /**
   * Builds the Read Context Cache {@link #tableReadContextCache}
   */
  @SuppressWarnings("unchecked")
  private LoadingCache<TableRuntimeContext, TableReadContext> buildReadContextCache(CacheLoader<TableRuntimeContext, TableReadContext> tableCacheLoader) {
    CacheBuilder resultSetCacheBuilder = CacheBuilder.newBuilder()
        .removalListener(new JdbcTableReadContextInvalidationListener());

    if (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      if (tableJdbcConfigBean.resultCacheSize > 0) {
        resultSetCacheBuilder = resultSetCacheBuilder.maximumSize(tableJdbcConfigBean.resultCacheSize);
      }
    } else {
      resultSetCacheBuilder = resultSetCacheBuilder.maximumSize(1);
    }

    if (tableCacheLoader != null) {
      return resultSetCacheBuilder.build(tableCacheLoader);
    } else {
      return resultSetCacheBuilder.build(new JdbcTableReadContextLoader(
          connectionManager,
          offsets,
          tableJdbcConfigBean.fetchSize,
          tableJdbcConfigBean.quoteChar.getQuoteCharacter(),
          tableJdbcELEvalContext
      ));
    }
  }

  /**
   * Initialize the gauge with needed information
   */
  private void initGaugeIfNeeded() {
    gaugeMap.put(THREAD_NAME, Thread.currentThread().getName());
    gaugeMap.put(STATUS, "");
    gaugeMap.put(TABLES_OWNED_COUNT, tableReadContextCache.size());
    gaugeMap.put(CURRENT_TABLE, "");
  }

  private void updateGauge(JdbcBaseRunnable.Status status) {
    gaugeMap.put(STATUS, status.name());
    gaugeMap.put(
        CURRENT_TABLE,
        Optional.ofNullable(tableRuntimeContext).map(TableRuntimeContext::getQualifiedName).orElse("")
    );
    gaugeMap.put(
        TABLES_OWNED_COUNT,
        tableProvider.getOwnedTablesQueue().size()
    );
  }

  /**
   * Generate a batch (looping through as many tables as needed) until a batch can be generated
   * and then commit offset.
   */
  private void generateBatchAndCommitOffset(BatchContext batchContext) {
    int recordCount = 0;
    try {
        while (tableRuntimeContext == null) {
          tableRuntimeContext = tableProvider.nextTable(threadNumber);
          if (tableRuntimeContext == null) {
            // small sleep before trying to acquire a table again, to potentially allow a new partition to be
            // returned to shared queue or created
            final boolean uninterrupted = ThreadUtil.sleep(ACQUIRE_TABLE_SLEEP_INTERVAL);
            if (!uninterrupted || tableRuntimeContext == null) {
              return;
            }
          }
        }
        updateGauge(JdbcBaseRunnable.Status.QUERYING_TABLE);
        TableReadContext tableReadContext = getOrLoadTableReadContext();
        ResultSet rs = tableReadContext.getResultSet();
        boolean resultSetEndReached = false;
        try {
          updateGauge(JdbcBaseRunnable.Status.GENERATING_BATCH);
          while (recordCount < batchSize) {
            if (rs.isClosed() || !rs.next()) {
              resultSetEndReached = true;
              break;
            }
            createAndAddRecord(rs, tableRuntimeContext, batchContext);
            recordCount++;
          }
          tableRuntimeContext.setResultSetProduced(true);

          //Reset numSqlErrors if we are able to read result set and add records to the batch context.
          numSQLErrors = 0;
          firstSqlException = null;

          //If exception happened we do not report anything about no more data event
          //We report noMoreData if either evictTableReadContext is true (result set no more rows) / record count is 0.
          tableProvider.reportDataOrNoMoreData(
              tableRuntimeContext,
              recordCount,
              batchSize,
              resultSetEndReached
          );
        } finally {
          handlePostBatchAsNeeded(resultSetEndReached, recordCount, batchContext);
        }
      } catch (SQLException | ExecutionException | StageException | InterruptedException e) {
        //invalidate if the connection is closed
        tableReadContextCache.invalidateAll();
        connectionManager.closeConnection();
        LOG.error("Error happened", e);
        Throwable th = (e instanceof ExecutionException)? e.getCause() : e;
        if (th instanceof SQLException) {
          handleSqlException((SQLException)th);
        } else if (e instanceof InterruptedException) {
          LOG.error("Thread {} interrupted", gaugeMap.get(THREAD_NAME));
        } else {
          handleStageError(JdbcErrors.JDBC_67, e);
        }
      }
    }

  private void handleSqlException(SQLException sqlE) {
    numSQLErrors++;
    if (numSQLErrors == 1) {
      firstSqlException = sqlE;
    }
    if (numSQLErrors < commonSourceConfigBean.numSQLErrorRetries) {
      LOG.error(
          "SQL Exception happened : {}, will retry. Retries Count : {}, Max Retries Count : {}",
          JdbcUtil.formatSqlException(sqlE),
          numSQLErrors,
          commonSourceConfigBean.numSQLErrorRetries
      );
    } else {
      LOG.error("Maximum SQL Error Retries {} exhausted", commonSourceConfigBean.numSQLErrorRetries);
      handleStageError(JdbcErrors.JDBC_78, firstSqlException);
    }
  }

  private void calculateEvictTableFlag(
      AtomicBoolean evictTableReadContext,
      TableReadContext tableReadContext
  ) {
    if (tableReadContext.isNeverEvict()) {
      evictTableReadContext.set(false);
      return;
    }
    boolean shouldEvict = evictTableReadContext.get();
    boolean isNumberOfBatchesReached = (
        tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES
            && tableJdbcConfigBean.numberOfBatchesFromRs > 0
            &&  tableReadContext.getNumberOfBatches() >= tableJdbcConfigBean.numberOfBatchesFromRs
    );
    evictTableReadContext.set(shouldEvict || isNumberOfBatchesReached);
  }

  /**
   * After a batch is generate perform needed operations, generate and commit batch
   * Evict entries from {@link #tableReadContextCache}, reset {@link #tableRuntimeContext}
   */
  private void handlePostBatchAsNeeded(
      boolean resultSetEndReached,
      int recordCount,
      BatchContext batchContext
  ) {
    AtomicBoolean shouldEvict = new AtomicBoolean(resultSetEndReached);
    //Only process batch if there are records
    if (recordCount > 0) {
      TableReadContext tableReadContext = tableReadContextCache.getIfPresent(tableRuntimeContext);
      Optional.ofNullable(tableReadContext)
          .ifPresent(readContext -> {
            readContext.setNumberOfBatches(readContext.getNumberOfBatches() + 1);
            LOG.debug(
                "Table {} read {} number of batches from the fetched result set",
                tableRuntimeContext.getQualifiedName(),
                readContext.getNumberOfBatches()
            );
            calculateEvictTableFlag(shouldEvict, tableReadContext);
          });
      updateGauge(JdbcBaseRunnable.Status.BATCH_GENERATED);

      //Process And Commit offsets
      if (tableRuntimeContext.isUsingNonIncrementalLoad()) {
        // process the batch now, will handle the offset commit outside this block
        context.processBatch(batchContext);
      } else {
        // for incremental (normal) mode, the offset was already stored in this map
        // by the specific subclass's createAndAddRecord method
        final String offsetValue = offsets.get(tableRuntimeContext.getOffsetKey());
        context.processBatch(batchContext, tableRuntimeContext.getOffsetKey(), offsetValue);
      }

    }

    if (tableRuntimeContext.isUsingNonIncrementalLoad()) {
      // for non-incremental mode, the offset is simply a singleton map indicating whether it's finished
      final String offsetValue = createNonIncrementalLoadOffsetValue(resultSetEndReached);
      context.commitOffset(tableRuntimeContext.getOffsetKey(), offsetValue);
    }

    //Make sure we close the result set only when there are no more rows in the result set
    if (shouldEvict.get()) {
      //Invalidate so as to fetch a new result set
      //We close the result set/statement in Removal Listener
      tableReadContextCache.invalidate(tableRuntimeContext);

      tableProvider.releaseOwnedTable(tableRuntimeContext, threadNumber);

      tableRuntimeContext = null;
    } else if (
        tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES
            && !tableRuntimeContext.isUsingNonIncrementalLoad()) {
      tableRuntimeContext = null;
    }

    final List<TableRuntimeContext> removedPartitions = tableProvider.getAndClearRemovedPartitions();
    if (removedPartitions != null && removedPartitions.size() > 0) {
      for (TableRuntimeContext partition : removedPartitions) {
        LOG.debug(
            "Removing offset entry for partition {} since it has been removed from the table provider",
            partition.getDescription()
        );
        context.commitOffset(partition.getOffsetKey(), null);
      }
    }
  }

  private static String createNonIncrementalLoadOffsetValue(boolean completed) {
    final Map<String, String> offsets = new HashMap<>();
    offsets.put(TableRuntimeContext.NON_INCREMENTAL_LOAD_OFFSET_COMPLETED_KEY, String.valueOf(completed));
    return OffsetQueryUtil.getSourceKeyOffsetsRepresentation(offsets);
  }

  /**
   * Initialize the {@link TableJdbcELEvalContext} before generating a batch
   */
  private static void initTableEvalContextForProduce(
      TableJdbcELEvalContext tableJdbcELEvalContext,
      TableRuntimeContext tableContext,
      Calendar calendar
  ) {
    tableJdbcELEvalContext.setCalendar(calendar);
    tableJdbcELEvalContext.setTime(calendar.getTime());
    tableJdbcELEvalContext.setTableContext(tableContext);
  }

  /**
   * Wait If needed before a new query is issued. (Does not wait if the result set is already cached)
   */
  private void waitIfNeeded() throws InterruptedException {
    long delayTime =
        (commonSourceConfigBean.queryInterval * 1000) - (System.currentTimeMillis() - lastQueryIntervalTime);
    Thread.sleep((lastQueryIntervalTime < 0 || delayTime < 0) ? 0 : delayTime);
  }


  /**
   * Get Or Load {@link TableReadContext} for {@link #tableRuntimeContext}
   */
  private TableReadContext getOrLoadTableReadContext() throws ExecutionException, InterruptedException {
    initTableEvalContextForProduce(
        tableJdbcELEvalContext, tableRuntimeContext,
        Calendar.getInstance(TimeZone.getTimeZone(tableJdbcConfigBean.timeZoneID))
    );
    //Check and then if we want to wait for query being issued do that
    TableReadContext tableReadContext = tableReadContextCache.getIfPresent(tableRuntimeContext);

    LOG.trace("Selected table : '{}' for generating records", tableRuntimeContext.getDescription());

    if (tableReadContext == null) {
      //Wait before issuing query (Optimization instead of waiting during each batch)
      waitIfNeeded();
      //Set time before query
      initTableEvalContextForProduce(
          tableJdbcELEvalContext, tableRuntimeContext,
          Calendar.getInstance(TimeZone.getTimeZone(tableJdbcConfigBean.timeZoneID))
      );
      tableReadContext = tableReadContextCache.get(tableRuntimeContext);
      //Record query time
      lastQueryIntervalTime = System.currentTimeMillis();
    }
    return tableReadContext;
  }

  /**
   * Handle Exception
   */
  private void handleStageError(ErrorCode errorCode, Exception e) {
    String errorMessage = (e instanceof SQLException)? JdbcUtil.formatSqlException((SQLException)e) : "Failure Happened";
    LOG.error(errorMessage, e);

    try {
      errorRecordHandler.onError(errorCode, e);
    } catch (StageException se) {
      LOG.error("Error when routing to stage error", se);
      //Way to throw stage exception from runnable to main source thread
      Throwables.propagate(se);
    }
  }



}
