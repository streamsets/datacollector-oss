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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcTableUtil;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.CDCJdbcRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.JdbcBaseRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.JdbcRunnableBuilder;
import com.streamsets.pipeline.lib.jdbc.multithread.MultithreadedTableProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderProviderFactory;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.cache.SQLServerCDCContextLoader;
import com.streamsets.pipeline.lib.util.OffsetUtil;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.SQLServerCTSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class SQLServerCDCSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerCTSource.class);
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.jdbc.CDC.sqlserver.SQLServerCDCSource.offset.version$";
  public static final String OFFSET_VERSION_1 = "1";
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private final HikariPoolConfigBean hikariConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final CDCTableJdbcConfigBean cdcTableJdbcConfigBean;
  private final Properties driverProperties = new Properties();
  private final Map<String, TableContext> allTableContexts;
  //If we have more state to clean up, we can introduce a state manager to do that which
  //can keep track of different closeables from different threads
  private final Collection<Cache<TableRuntimeContext, TableReadContext>> toBeInvalidatedThreadCaches;

  private int numberOfThreads;
  private ConnectionManager connectionManager;
  private Map<String, String> offsets;
  private ExecutorService executorService;
  private MultithreadedTableProvider tableOrderProvider;
  private HikariDataSource dataSource;

  public SQLServerCDCSource(HikariPoolConfigBean hikariConfigBean, CommonSourceConfigBean commonSourceConfigBean, CDCTableJdbcConfigBean tableJdbcConfigBean) {
    this.hikariConfigBean = hikariConfigBean;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.cdcTableJdbcConfigBean = tableJdbcConfigBean;
    allTableContexts = new LinkedHashMap<>();
    toBeInvalidatedThreadCaches = new ArrayList<>();
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    PushSource.Context context = getContext();
    issues = hikariConfigBean.validateConfigs(context, issues);
    issues = commonSourceConfigBean.validateConfigs(context, issues);
    issues = cdcTableJdbcConfigBean.validateConfigs(context, issues);

    //Max pool size should be equal to number of threads
    //The main thread will use one connection to list threads and close (return to hikari pool) the connection
    // and each individual data threads needs one connection
    if (cdcTableJdbcConfigBean.numberOfThreads > hikariConfigBean.maximumPoolSize) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ADVANCED.name(),
              HIKARI_CONFIG_PREFIX + HikariPoolConfigBean.MAX_POOL_SIZE_NAME,
              JdbcErrors.JDBC_74,
              hikariConfigBean.maximumPoolSize,
              cdcTableJdbcConfigBean.numberOfThreads
          )
      );
    }

    if (issues.isEmpty()) {
      checkConnectionAndBootstrap(context, issues);
    }
    return issues;
  }

  @VisibleForTesting
  void checkConnectionAndBootstrap(Stage.Context context, List<ConfigIssue> issues) {
    try {
      dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
    } catch (StageException e) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
    }

    if (issues.isEmpty()) {
      try {
        connectionManager = new ConnectionManager(dataSource);
        for (CDCTableConfigBean tableConfigBean : cdcTableJdbcConfigBean.tableConfigs) {
          //No duplicates even though a table matches multiple configurations, we will add it only once.
          allTableContexts.putAll(
              TableContextUtil.listCDCTablesForConfig(
                  connectionManager.getConnection(),
                  tableConfigBean
              )
          );
        }

        LOG.info("Selected Tables: \n {}", NEW_LINE_JOINER.join(allTableContexts.keySet()));

        if (allTableContexts.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  Groups.TABLE.name(),
                  TableJdbcConfigBean.TABLE_CONFIG,
                  JdbcErrors.JDBC_66
              )
          );
        } else {
          numberOfThreads = cdcTableJdbcConfigBean.numberOfThreads;
          if (cdcTableJdbcConfigBean.numberOfThreads > allTableContexts.size()) {
            numberOfThreads = Math.min(cdcTableJdbcConfigBean.numberOfThreads, allTableContexts.size());
            LOG.info(
                "Number of threads configured '{}'is more than number of tables '{}'. Will be Using '{}' number of threads.",
                cdcTableJdbcConfigBean.numberOfThreads,
                allTableContexts.size(),
                numberOfThreads
            );
          }

          TableOrderProvider tableOrderProvider = new TableOrderProviderFactory(
              connectionManager.getConnection(),
              cdcTableJdbcConfigBean.tableOrderStrategy
          ).create();

          try {
            tableOrderProvider.initialize(allTableContexts);
            this.tableOrderProvider = new MultithreadedTableProvider(
                allTableContexts,
                tableOrderProvider.getOrderedTables(),
                JdbcTableUtil.decideMaxTableSlotsForThreads(cdcTableJdbcConfigBean.batchTableStrategy, allTableContexts.size(), numberOfThreads),
                numberOfThreads,
                cdcTableJdbcConfigBean.batchTableStrategy
            );
          } catch (ExecutionException e) {
            LOG.error("Error during Table Order Provider Init", e);
            throw new StageException(JdbcErrors.JDBC_67, e);
          }
          //Accessed by all runner threads
          offsets = new ConcurrentHashMap<>();
        }
      } catch (SQLException e) {
        JdbcUtil.logError(e);
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      } catch (StageException e) {
        LOG.error("Error when finding tables:", e);
        issues.add(
            context.createConfigIssue(
                Groups.TABLE.name(),
                TableJdbcConfigBean.TABLE_CONFIG,
                e.getErrorCode(),
                e.getParams()
            )
        );
      } finally {
        Optional.ofNullable(connectionManager).ifPresent(ConnectionManager::closeConnection);
      }
    }
  }

  private void shutdownExecutorIfNeeded() {
    Optional.ofNullable(executorService).ifPresent(executor -> {
      if (!executor.isTerminated()) {
        LOG.info("Shutting down executor service");
        executor.shutdown();
      }
    });
  }

  @Override
  public void destroy() {
    executorService = null;
    //Invalidate all the thread cache so that all statements/result sets are properly closed.
    toBeInvalidatedThreadCaches.forEach(Cache::invalidateAll);
    //Closes all connections
    Optional.ofNullable(connectionManager).ifPresent(ConnectionManager::closeAll);
    JdbcUtil.closeQuietly(dataSource);
  }

  @Override
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  @Override
  public void produce(Map<String, String> lastSourceOffset, int maxBatchSize) throws StageException {
    int batchSize = Math.min(maxBatchSize, commonSourceConfigBean.maxBatchSize);

    handleLastOffset(lastSourceOffset);
    try {
      executorService = new SafeScheduledExecutorService(numberOfThreads, JdbcBaseRunnable.TABLE_JDBC_THREAD_PREFIX);

      ExecutorCompletionService<Future> completionService = new ExecutorCompletionService<>(executorService);

      IntStream.range(0, numberOfThreads).forEach(threadNumber -> {
        CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache = new SQLServerCDCContextLoader(
            connectionManager,
            lastSourceOffset,
            cdcTableJdbcConfigBean.fetchSize
        );

        TableJdbcConfigBean tableJdbcConfigBean = convertToTableJdbcConfigBean(cdcTableJdbcConfigBean);

        JdbcBaseRunnable runnable = new JdbcRunnableBuilder().context(getContext())
            .threadNumber(threadNumber)
            .batchSize(batchSize)
            .connectionManager(connectionManager)
            .offsets(offsets)
            .tableProvider(tableOrderProvider)
            .tableReadContextCache(tableReadContextCache)
            .commonSourceConfigBean(commonSourceConfigBean)
            .tableJdbcConfigBean(tableJdbcConfigBean)
            .build();
        toBeInvalidatedThreadCaches.add(runnable.getTableReadContextCache());
        completionService.submit(runnable, null);
      });

      while (!getContext().isStopped()) {
        checkWorkerStatus(completionService);
        JdbcUtil.generateNoMoreDataEventIfNeeded(tableOrderProvider.shouldGenerateNoMoreDataEvent(), getContext());
      }
    } catch (Exception e) {
      LOG.error("Exception thrown during produce", e);
    } finally {
      shutdownExecutorIfNeeded();
    }
  }

  /**
   * Checks whether any of the {@link CDCJdbcRunnable} workers completed
   * and whether there is any error that needs to be handled from them.
   * @param completionService {@link ExecutorCompletionService} used to detect completion
   * @throws StageException if {@link StageException} is thrown by the workers (if the error handling is stop pipeline)
   */
  private void checkWorkerStatus(ExecutorCompletionService<Future> completionService) throws StageException {
    Future future = completionService.poll();
    if (future != null) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        Throwable cause = Throwables.getRootCause(e);
        if (cause != null && cause instanceof StageException) {
          throw (StageException) cause;
        } else {
          LOG.error("Internal Error. {}", e);
          throw new StageException(JdbcErrors.JDBC_75, e.toString());
        }
      }
    }
  }

  void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets != null) {
      if (lastOffsets.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
        String innerTableOffsetsAsString = lastOffsets.get(Source.POLL_SOURCE_OFFSET_KEY);

        if (innerTableOffsetsAsString != null) {
          try {
            offsets.putAll(OffsetUtil.deserializeOffsetMap(innerTableOffsetsAsString));
          } catch (IOException ex) {
            LOG.error("Error when deserializing", ex);
            throw new StageException(JdbcErrors.JDBC_61, ex);
          }
        }

        offsets.forEach((tableName, tableOffset) -> getContext().commitOffset(tableName, tableOffset));

        //Remove Poll Source Offset key from the offset.
        //Do this at last so as not to lose the offsets if there is failure in the middle
        //when we call commitOffset above
        getContext().commitOffset(Source.POLL_SOURCE_OFFSET_KEY, null);
      } else {
        offsets.putAll(lastOffsets);
      }
      //Only if it is not already committed
      if (!lastOffsets.containsKey(OFFSET_VERSION)) {
        //Version the offset so as to allow for future evolution.
        getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_1);
      }
    }

    //If the offset already does not contain the table (meaning it is the first start or a new table)
    //We can skip validation
    for (Map.Entry<String, String> tableAndOffsetEntry : offsets.entrySet()) {
      TableContext tableContext = allTableContexts.get(tableAndOffsetEntry.getKey());
      if (tableContext != null) { //When the table is removed from the configuration
        try {
          OffsetQueryUtil.validateStoredAndSpecifiedOffset(tableContext, tableAndOffsetEntry.getValue());
        } catch (StageException e) {
          LOG.error("Error when validating stored offset with configuration", e);
          //Throw the stage exception, we should not start the pipeline with this.
          throw e;
        }
      }
    }
  }

  private TableJdbcConfigBean convertToTableJdbcConfigBean(CDCTableJdbcConfigBean ctTableJdbcConfigBean) {
    TableJdbcConfigBean tableJdbcConfigBean = new TableJdbcConfigBean();
    tableJdbcConfigBean.batchTableStrategy = ctTableJdbcConfigBean.batchTableStrategy;
    tableJdbcConfigBean.timeZoneID = ctTableJdbcConfigBean.timeZoneID;
    tableJdbcConfigBean.quoteChar = QuoteChar.NONE;

    return tableJdbcConfigBean;
  }
}
