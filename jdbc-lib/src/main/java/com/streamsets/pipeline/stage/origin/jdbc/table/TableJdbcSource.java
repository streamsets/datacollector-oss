/**
 * Copyright 2016 StreamSets Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.util.OffsetQueryUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class TableJdbcSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSource.class);
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String OFFSET_VERSION = "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$";
  private static final String OFFSET_VERSION_1 = "1";

  private final HikariPoolConfigBean hikariConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final Properties driverProperties = new Properties();
  //If we have more state to clean up, we can introduce a state manager to do that which
  //can keep track of different closeables from different threads
  private final Collection<Cache<TableContext, TableReadContext>> toBeInvalidatedThreadCaches;

  private Calendar calendar;
  private HikariDataSource hikariDataSource;
  private ConnectionManager connectionManager;
  private Map<String, String> offsets;
  private ExecutorService executorService;
  private MultithreadedTableProvider tableOrderProvider;
  private int numberOfThreads;

  public TableJdbcSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean
  ) {
    this.hikariConfigBean = hikariConfigBean;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.tableJdbcConfigBean = tableJdbcConfigBean;
    driverProperties.putAll(hikariConfigBean.driverProperties);
    toBeInvalidatedThreadCaches = new ArrayList<>();
  }

  private static String logError(SQLException e) {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.debug(formattedError, e);
    return formattedError;
  }

  @VisibleForTesting
  void checkConnectionAndBootstrap(Stage.Context context, List<ConfigIssue> issues) {
    try {
      hikariDataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
    } catch (StageException e) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
    }
    if (issues.isEmpty()) {
      try {
        calendar = Calendar.getInstance(TimeZone.getTimeZone(tableJdbcConfigBean.timeZoneID));

        connectionManager = new ConnectionManager(hikariDataSource);

        Map<String, TableContext> allTableContexts = new LinkedHashMap<>();

        for (TableConfigBean tableConfigBean : tableJdbcConfigBean.tableConfigs) {
          //No duplicates even though a table matches multiple configurations, we will add it only once.
          allTableContexts.putAll(
              TableContextUtil.listTablesForConfig(
                  connectionManager.getConnection(),
                  tableConfigBean,
                  new TableJdbcELEvalContext(context, context.createELVars())
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
          numberOfThreads = tableJdbcConfigBean.numberOfThreads;
          if (tableJdbcConfigBean.numberOfThreads > allTableContexts.size()) {
            numberOfThreads = Math.min(tableJdbcConfigBean.numberOfThreads, allTableContexts.size());
            LOG.info(
                "Number of threads configured '{}'is more than number of tables '{}'. Will be Using '{}' number of threads.",
                tableJdbcConfigBean.numberOfThreads,
                allTableContexts.size(),
                numberOfThreads
            );
          }

          TableOrderProvider tableOrderProvider = new TableOrderProviderFactory(
              connectionManager.getConnection(),
              tableJdbcConfigBean.tableOrderStrategy
          ).create();

          try {
            tableOrderProvider.initialize(allTableContexts);
            int maxQueueSize =
                (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) ?
                    allTableContexts.size() / numberOfThreads
                    : 1;
            this.tableOrderProvider = new MultithreadedTableProvider(
                allTableContexts,
                tableOrderProvider.getOrderedTables(),
                maxQueueSize
            );
          } catch (ExecutionException e) {
            LOG.debug("Error during Table Order Provider Init", e);
            throw new StageException(JdbcErrors.JDBC_67, e);
          }
          //Accessed by all runner threads
          offsets = new ConcurrentHashMap<>();
        }
      } catch (SQLException e) {
        logError(e);
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      } catch (StageException e) {
        LOG.debug("Error when finding tables:", e);
        issues.add(
            context.createConfigIssue(
                Groups.TABLE.name(),
                TableJdbcConfigBean.TABLE_CONFIG,
                e.getErrorCode(),
                e.getParams()
            )
        );
      } finally {
        Optional.ofNullable(connectionManager).ifPresent(ConnectionManager::closeAll);
      }
    }
  }

  @Override
  protected List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    PushSource.Context context = getContext();
    issues = hikariConfigBean.validateConfigs(context, issues);
    issues = commonSourceConfigBean.validateConfigs(context, issues);
    issues = tableJdbcConfigBean.validateConfigs(context, issues);

    //Max pool size should be at least one greater than number of threads
    //The main thread needs one connection and each individual data threads needs
    //one connection
    if (tableJdbcConfigBean.numberOfThreads >= hikariConfigBean.maximumPoolSize) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ADVANCED.name(),
              "hikariConfigBean." + HikariPoolConfigBean.MAX_POOL_SIZE_NAME,
              JdbcErrors.JDBC_74,
              hikariConfigBean.maximumPoolSize,
              tableJdbcConfigBean.numberOfThreads
          )
      );
    }

    if (issues.isEmpty()) {
      checkConnectionAndBootstrap(context, issues);
    }
    return issues;
  }

  @Override
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    int batchSize = Math.min(maxBatchSize, commonSourceConfigBean.maxBatchSize);
    handleLastOffset(lastOffsets);
    try {
      executorService = new SafeScheduledExecutorService(numberOfThreads, TableJdbcRunnable.TABLE_JDBC_THREAD_PREFIX);

      List<Future> futures = new ArrayList<>();

      IntStream.range(0, numberOfThreads).forEach(threadNumber -> {
        TableJdbcRunnable runnable = new TableJdbcRunnable.Builder()
            .context(getContext())
            .threadNumber(threadNumber)
            .calendar(calendar)
            .batchSize(batchSize)
            .connectionManager(connectionManager)
            .offsets(offsets)
            .tableProvider(tableOrderProvider)
            .commonSourceConfigBean(commonSourceConfigBean)
            .tableJdbcConfigBean(tableJdbcConfigBean)
            .build();
        toBeInvalidatedThreadCaches.add(runnable.getTableReadContextCache());
        futures.add(executorService.submit(runnable));
      });

      while (!getContext().isStopped()) {
        for (Future future : futures) {
          if (future.isDone()) {
            try {
              future.get();
            } catch (InterruptedException e) {
              LOG.error("Thread interrupted", e);
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
      }
    } finally {
      connectionManager.closeConnection();
      shutdownExecutorIfNeeded();
    }
  }

  @Override
  public void destroy() {
    shutdownExecutorIfNeeded();
    executorService = null;
    //Invalidate all the thread cache so that all statements/result sets are properly closed.
    toBeInvalidatedThreadCaches.forEach(Cache::invalidateAll);
    //Closes all connections
    Optional.ofNullable(connectionManager).ifPresent(ConnectionManager::closeAll);
    JdbcUtil.closeQuietly(hikariDataSource);
  }

  private void shutdownExecutorIfNeeded() {
    Optional.ofNullable(executorService).ifPresent(executor -> {
      if (!executor.isTerminated()) {
        LOG.info("Shutting down executor service");
        executor.shutdown();
      }
    });
  }

  private void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets != null) {
      if (lastOffsets.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
        String innerTableOffsetsAsString = lastOffsets.get(Source.POLL_SOURCE_OFFSET_KEY);

        if (innerTableOffsetsAsString != null) {
          offsets.putAll(OffsetQueryUtil.deserializeOffsetMap(innerTableOffsetsAsString));
        }

        offsets.forEach((tableName, tableOffset) -> getContext().commitOffset(tableName, tableOffset));

        //Remove Poll Source Offset key from the offset.
        //Do this at last so as not to lose the offsets if there is failure in the middle
        //when we call commitOffset above
        getContext().commitOffset(Source.POLL_SOURCE_OFFSET_KEY, null);

        //Version the offset so as to allow for future evolution.
        getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_1);
      } else {
        offsets.putAll(lastOffsets);
      }
    }
  }
}
