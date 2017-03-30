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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.cache.JdbcTableReadContextInvalidationListener;
import com.streamsets.pipeline.stage.origin.jdbc.table.cache.JdbcTableReadContextLoader;
import com.streamsets.pipeline.stage.origin.jdbc.table.util.OffsetQueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TableJdbcRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcRunnable.class);
  private static final String JDBC_NAMESPACE_HEADER = "jdbc.";

  static final String THREAD_NAME = "Thread Name";
  static final String CURRENT_TABLE = "Current Table";
  static final String TABLES_OWNED_COUNT = "Tables Owned";
  static final String TABLE_METRICS = "Table Metrics for Thread - ";
  static final String TABLE_JDBC_THREAD_PREFIX = "Table Jdbc Runner - ";

  private final PushSource.Context context;
  private final Map<String, String> offsets;
  private final LoadingCache<TableContext, TableReadContext> tableReadContextCache;
  private final MultithreadedTableProvider tableProvider;
  private final int threadNumber;
  private final int batchSize;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcELEvalContext tableJdbcELEvalContext;
  private final ConnectionManager connectionManager;
  private final ErrorRecordHandler errorRecordHandler;
  private final Map<String, Object> gaugeMap;

  private TableContext tableContext;
  private long lastQueryIntervalTime;

  TableJdbcRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, String> offsets,
      MultithreadedTableProvider tableProvider,
      ConnectionManager connectionManager,
      TableJdbcConfigBean tableJdbcConfigBean,
      CommonSourceConfigBean commonSourceConfigBean
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
    this.tableReadContextCache = buildReadContextCache();
    this.errorRecordHandler = new DefaultErrorRecordHandler(context, (ToErrorContext) context);

    // Metrics
    String gaugeName = TABLE_METRICS + threadNumber;
    this.gaugeMap = context.createGauge(gaugeName).getValue();
  }

  LoadingCache<TableContext, TableReadContext> getTableReadContextCache() {
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
  private LoadingCache<TableContext, TableReadContext> buildReadContextCache() {
    CacheBuilder resultSetCacheBuilder = CacheBuilder.newBuilder()
        .removalListener(new JdbcTableReadContextInvalidationListener());

    if (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      if (tableJdbcConfigBean.resultCacheSize > 0) {
        resultSetCacheBuilder = resultSetCacheBuilder.maximumSize(tableJdbcConfigBean.resultCacheSize);
      }
    } else {
      resultSetCacheBuilder = resultSetCacheBuilder.maximumSize(1);
    }

    return resultSetCacheBuilder.build(
        new JdbcTableReadContextLoader(
            connectionManager,
            offsets,
            tableJdbcConfigBean.fetchSize,
            tableJdbcConfigBean.quoteChar.getQuoteCharacter(),
            tableJdbcELEvalContext
        )
    );
  }

  /**
   * Initialize the gauge with needed information
   */
  private void initGaugeIfNeeded() {
    gaugeMap.put(THREAD_NAME, Thread.currentThread().getName());
    gaugeMap.put(TABLES_OWNED_COUNT, tableReadContextCache.size());
    gaugeMap.put(CURRENT_TABLE, "");
  }

  private void updateGauge() {
    gaugeMap.put(CURRENT_TABLE, tableContext.getQualifiedName());
    gaugeMap.put(TABLES_OWNED_COUNT, tableReadContextCache.size());
  }

  /**
   * Generate a batch (looping through as many tables as needed) until a batch can be generated
   * and then commit offset.
   */
  private void generateBatchAndCommitOffset(BatchContext batchContext) {
    int recordCount = 0;
    try {
      if (tableContext == null) {
        tableContext = tableProvider.nextTable(threadNumber);
      }
      TableReadContext tableReadContext = getOrLoadTableReadContext();
      ResultSet rs = tableReadContext.getResultSet();
      boolean evictTableReadContext = false;
      try {
        updateGauge();
        while (recordCount < batchSize) {
          if (rs.isClosed() || !rs.next()) {
            evictTableReadContext = true;
            break;
          }
          createAndAddRecord(rs, tableContext, batchContext);
          recordCount++;
        }
        //If exception happened we do not report anything about no more data event
        //We report noMoreData if either evictTableReadContext is true (result set no more rows) / record count is 0.
        tableProvider.reportDataOrNoMoreData(tableContext, recordCount == 0 || evictTableReadContext);
      } finally {
        handlePostBatchAsNeeded(new AtomicBoolean(evictTableReadContext), recordCount, batchContext);
      }
    } catch (SQLException | ExecutionException | StageException | InterruptedException e) {
      //invalidate if the connection is closed
      tableReadContextCache.invalidateAll();
      connectionManager.closeConnection();
      if (e instanceof SQLException) {
        handleStageError(JdbcErrors.JDBC_34, e);
      } else if (e instanceof InterruptedException) {
        LOG.error("Thread {} interrupted", gaugeMap.get(THREAD_NAME));
      } else {
        handleStageError(JdbcErrors.JDBC_67, e);
      }
    }
  }

  private void calculateEvictTableFlag(AtomicBoolean evictTableReadContext, TableReadContext tableReadContext) {
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
   * Evict entries from {@link #tableReadContextCache}, reset {@link #tableContext}
   */
  private void handlePostBatchAsNeeded(AtomicBoolean shouldEvict, int recordCount, BatchContext batchContext) {
    //Only process batch if there are records
    if (recordCount > 0) {
      TableReadContext tableReadContext = tableReadContextCache.getIfPresent(tableContext);
      Optional.ofNullable(tableReadContext)
          .ifPresent(readContext -> {
            readContext.setNumberOfBatches(readContext.getNumberOfBatches() + 1);
            LOG.debug(
                "Table {} read {} number of batches from the fetched result set",
                tableContext.getQualifiedName(),
                readContext.getNumberOfBatches()
            );
            calculateEvictTableFlag(shouldEvict, tableReadContext);
          });
      //Process And Commit offsets
      context.processBatch(batchContext, tableContext.getQualifiedName(), offsets.get(tableContext.getQualifiedName()));
    }
    //Make sure we close the result set only when there are no more rows in the result set
    if (shouldEvict.get()) {
      //Invalidate so as to fetch a new result set
      //We close the result set/statement in Removal Listener
      tableReadContextCache.invalidate(tableContext);

      //evict from owned tables
      tableProvider.releaseOwnedTable(tableContext);

      tableContext = null;
    } else if (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      tableContext = null;
    }
  }

  /**
   * Initialize the {@link TableJdbcELEvalContext} before generating a batch
   */
  private static void initTableEvalContextForProduce(
      TableJdbcELEvalContext tableJdbcELEvalContext,
      TableContext tableContext,
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
   * Get Or Load {@link TableReadContext} for {@link #tableContext}
   */
  private TableReadContext getOrLoadTableReadContext() throws ExecutionException, InterruptedException {
    initTableEvalContextForProduce(
        tableJdbcELEvalContext,
        tableContext,
        Calendar.getInstance(TimeZone.getTimeZone(tableJdbcConfigBean.timeZoneID))
    );
    //Check and then if we want to wait for query being issued do that
    TableReadContext tableReadContext = tableReadContextCache.getIfPresent(tableContext);

    if (tableReadContext == null) {
      //Wait before issuing query (Optimization instead of waiting during each batch)
      waitIfNeeded();
      //Set time before query
      initTableEvalContextForProduce(
          tableJdbcELEvalContext,
          tableContext,
          Calendar.getInstance(TimeZone.getTimeZone(tableJdbcConfigBean.timeZoneID))
      );
      LOG.debug("Selected table : '{}' for generating records", tableContext.getQualifiedName());
      tableReadContext = tableReadContextCache.get(tableContext);
      //Record query time
      lastQueryIntervalTime = System.currentTimeMillis();
    }
    return tableReadContext;
  }

  /**
   * Create record and add it to {@link com.streamsets.pipeline.api.BatchMaker}
   */
  private void createAndAddRecord(
      ResultSet rs,
      TableContext tableContext,
      BatchContext batchContext
  ) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();

    LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(
        rs,
        commonSourceConfigBean.maxClobSize,
        commonSourceConfigBean.maxBlobSize,
        errorRecordHandler
    );

    String offsetFormat = OffsetQueryUtil.getOffsetFormatFromColumns(tableContext, fields);
    Record record = context.createRecord(tableContext.getQualifiedName() + ":" + offsetFormat);
    record.set(Field.createListMap(fields));

    //Set Column Headers
    JdbcUtil.setColumnSpecificHeaders(
        record,
        Collections.singleton(tableContext.getTableName()),
        md,
        JDBC_NAMESPACE_HEADER
    );

    batchContext.getBatchMaker().addRecord(record);

    offsets.put(tableContext.getQualifiedName(), offsetFormat);
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

  /**
   * Builder class for building a {@link TableJdbcRunnable}
   */
  static class Builder {
    private PushSource.Context context;
    private int threadNumber;
    private int batchSize;
    private TableJdbcConfigBean tableJdbcConfigBean;
    private CommonSourceConfigBean commonSourceConfigBean;
    private Map<String, String> offsets;
    private ConnectionManager connectionManager;
    private MultithreadedTableProvider tableProvider;

    public Builder() {
    }

    Builder context(PushSource.Context context) {
      this.context = context;
      return this;
    }

    Builder threadNumber(int threadNumber) {
      this.threadNumber = threadNumber;
      return this;
    }

    Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    Builder offsets(Map<String, String> offsets) {
      this.offsets = offsets;
      return this;
    }

    Builder tableProvider(MultithreadedTableProvider tableProvider) {
      this.tableProvider = tableProvider;
      return this;
    }

    Builder connectionManager(ConnectionManager connectionManager) {
      this.connectionManager = connectionManager;
      return this;
    }

    Builder tableJdbcConfigBean(TableJdbcConfigBean tableJdbcConfigBean) {
      this.tableJdbcConfigBean = tableJdbcConfigBean;
      return this;
    }

    Builder commonSourceConfigBean(CommonSourceConfigBean commonSourceConfigBean) {
      this.commonSourceConfigBean = commonSourceConfigBean;
      return this;
    }

    TableJdbcRunnable build() {
      return new TableJdbcRunnable(
          context,
          threadNumber,
          batchSize,
          offsets,
          tableProvider,
          connectionManager,
          tableJdbcConfigBean,
          commonSourceConfigBean
      );
    }
  }
}