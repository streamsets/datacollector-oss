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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public final class TableJdbcRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcRunnable.class);
  private static final String JDBC_NAMESPACE_HEADER = "jdbc.";

  static final String THREAD_NAME = "Thread Name";
  static final String CURRENT_TABLE = "Current Table";
  static final String TABLE_COUNT = "Table Count";
  static final String TABLE_METRICS = "Table Metrics for Thread - ";
  static final String TABLE_JDBC_THREAD_PREFIX = "Table Jdbc Runner -";

  private final PushSource.Context context;
  private final Map<String, String> offsets;
  private final LoadingCache<TableContext, TableReadContext> tableReadContextCache;
  private final int threadNumber;
  private final int batchSize;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final Calendar calendar;
  private final TableJdbcELEvalContext tableJdbcELEvalContext;
  private final ConnectionManager connectionManager;
  private final Map<String, TableContext> tableContexts;
  private final ErrorRecordHandler errorRecordHandler;
  private final Map<String, Object> gaugeMap;

  private TableOrderProvider tableOrderProvider;
  private TableContext tableContext;
  private long lastQueryIntervalTime;

  TableJdbcRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Calendar calendar,
      Map<String, TableContext> tableContexts,
      Map<String, String> offsets,
      ConnectionManager connectionManager,
      TableJdbcConfigBean tableJdbcConfigBean,
      CommonSourceConfigBean commonSourceConfigBean
  ) {
    this.context = context;
    this.threadNumber = threadNumber;
    this.lastQueryIntervalTime = -1;
    this.batchSize = batchSize;
    this.calendar = calendar;
    this.tableJdbcELEvalContext = new TableJdbcELEvalContext(context, context.createELVars());
    this.tableContexts = tableContexts;
    this.offsets = offsets;
    this.tableJdbcConfigBean = tableJdbcConfigBean;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.connectionManager = connectionManager;
    this.tableReadContextCache = buildReadContextCache();
    this.errorRecordHandler = new DefaultErrorRecordHandler(context, (ToErrorContext) context);

    // Metrics
    String gaugeName = TABLE_METRICS + threadNumber;
    this.gaugeMap = context.createGauge(gaugeName).getValue();
  }

  public LoadingCache<TableContext, TableReadContext> getTableReadContextCache() {
    return tableReadContextCache;
  }

  @Override
  public void run() {
    try {
      performPreRun();
      while (!context.isStopped()) {
        generateBatchAndCommitOffset(context.startBatch());
      }
    } catch (SQLException | ExecutionException | StageException e) {
      handleStageError(JdbcErrors.JDBC_67, e);
    }
  }

  /**
   * Init the gauge, init table order provider.
   */
  private void performPreRun() throws SQLException, ExecutionException, StageException {
    Thread.currentThread().setName(TABLE_JDBC_THREAD_PREFIX + threadNumber);
    TableOrderProviderFactory tableOrderProviderFactory =
        new TableOrderProviderFactory(connectionManager.getConnection(), tableJdbcConfigBean.tableOrderStrategy);
    this.tableOrderProvider = tableOrderProviderFactory.create();
    tableOrderProvider.initialize(tableContexts);
    initGaugeIfNeeded(context);
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
            tableJdbcELEvalContext
        )
    );
  }

  /**
   * Initialize the gauge with needed information
   */
  private void initGaugeIfNeeded(Stage.Context context) {
    gaugeMap.put(THREAD_NAME, Thread.currentThread().getName());
    gaugeMap.put(TABLE_COUNT, tableContexts.size());
    gaugeMap.put(CURRENT_TABLE, "");
  }

  /**
   * Generate a batch (looping through as many tables as needed) until a batch can be generated
   * and then commit offset.
   */
  private void generateBatchAndCommitOffset(BatchContext batchContext) {
    int recordCount = 0;
    try {
      initTableContextIfNeeded();
      if (tableContext != null) {
        TableReadContext tableReadContext = getOrLoadTableReadContext();
        ResultSet rs = tableReadContext.getResultSet();
        boolean evictTableReadContext = false;
        try {
          gaugeMap.put(CURRENT_TABLE, tableContext.getQualifiedName());
          // Create new batch
          while (recordCount < batchSize) {
            if (rs.isClosed() || !rs.next()) {
              evictTableReadContext = true;
              break;
            }
            createAndAddRecord(rs, tableContext, batchContext);
            recordCount++;
          }
        } finally {
          handlePostBatchAsNeeded(evictTableReadContext, recordCount, batchContext);
        }
      }
    } catch (SQLException | ExecutionException | StageException e) {
      connectionManager.closeConnection();
      if (e instanceof SQLException) {
        handleStageError(JdbcErrors.JDBC_34, e);
      } else {
        handleStageError(JdbcErrors.JDBC_67, e);
      }
    }
  }

  /**
   * After a batch is generate perform needed operations, generate and commit batch
   * Evict entries from {@link #tableReadContextCache}, reset {@link #tableContext}
   */
  private void handlePostBatchAsNeeded(boolean evictTableReadContext, int recordCount, BatchContext batchContext) {
    //Only process batch if there are records
    if (recordCount > 0) {
      //Process And Commit offsets
      context.processBatch(batchContext, tableContext.getQualifiedName(), offsets.get(tableContext.getQualifiedName()));
    }
    //Make sure we close the result set only when there are no more rows in the result set
    if (evictTableReadContext) {
      //Invalidate so as to fetch a new result set
      //We close the result set/statement in Removal Listener
      tableReadContextCache.invalidate(tableContext);
      tableContext = null;
    } else if (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      tableContext = null;
    }
  }

  /**
   * Initialize the {@link TableJdbcELEvalContext} before generating a batch
   */
  private void initTableEvalContextForProduce(TableContext tableContext) {
    tableJdbcELEvalContext.setCalendar(calendar);
    tableJdbcELEvalContext.setTime(calendar.getTime());
    tableJdbcELEvalContext.setTableContext(tableContext);
  }

  /**
   * Initialize and set the table context.
   * NOTE: This sets the table context to null if there is any mismatch
   * We will simply proceed to next table if that is the case
   */
  private void initTableContextIfNeeded() throws ExecutionException, SQLException, StageException {
    if (tableContext == null) {
      tableContext = tableOrderProvider.nextTable();
      //If the offset already does not contain the table (meaning it is the first start or a new table)
      //We can skip validation
      if (offsets.containsKey(tableContext.getQualifiedName())) {
        try {
          OffsetQueryUtil.validateStoredAndSpecifiedOffset(tableContext, offsets.get(tableContext.getQualifiedName()));
        } catch (StageException e) {
          LOG.error("Error when validating stored offset with configuration", e);
          tableContext = null;
          handleStageError(e.getErrorCode(), e);
        }
      }
    }
  }

  /**
   * Wait If needed before a new query is issued. (Does not wait if the result set is already cached)
   */
  private void waitIfNeeded() {
    long delayTime =
        (commonSourceConfigBean.queryInterval * 1000) - (System.currentTimeMillis() - lastQueryIntervalTime);
    ThreadUtil.sleep((lastQueryIntervalTime < 0 || delayTime < 0) ? 0 : delayTime);
  }


  /**
   * Get Or Load {@link TableReadContext} for {@link #tableContext}
   */
  private TableReadContext getOrLoadTableReadContext() throws ExecutionException {
    initTableEvalContextForProduce(tableContext);

    //Check and then if we want to wait for query being issued do that
    TableReadContext tableReadContext = tableReadContextCache.getIfPresent(tableContext);

    if (tableReadContext == null) {
      //Wait before issuing query (Optimization instead of waiting during each batch)
      waitIfNeeded();
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
    LOG.error("Failure happened when fetching nextTable", e);
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
    private Calendar calendar;
    private TableJdbcConfigBean tableJdbcConfigBean;
    private CommonSourceConfigBean commonSourceConfigBean;
    private Map<String, TableContext> tableContexts;
    private Map<String, String> offsets;
    private ConnectionManager connectionManager;

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

    Builder calendar(Calendar calendar) {
      this.calendar = calendar;
      return this;
    }

    Builder tableContexts(Map<String, TableContext> tableContexts) {
      this.tableContexts = tableContexts;
      return this;
    }

    Builder offsets(Map<String, String> offsets) {
      this.offsets = offsets;
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
          calendar,
          tableContexts,
          offsets,
          connectionManager,
          tableJdbcConfigBean,
          commonSourceConfigBean
      );
    }
  }
}