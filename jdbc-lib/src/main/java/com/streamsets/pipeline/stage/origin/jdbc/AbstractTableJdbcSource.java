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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.JdbcBaseRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.JdbcRunnableBuilder;
import com.streamsets.pipeline.lib.jdbc.multithread.MultithreadedTableProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableJdbcRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderProviderFactory;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractTableJdbcSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTableJdbcSource.class);
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private final HikariPoolConfigBean hikariConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private Map<String, TableContext> allTableContexts;
  private final Map<String, Integer> qualifiedTableNameToConfigIndex;
  //If we have more state to clean up, we can introduce a state manager to do that which
  //can keep track of different closeables from different threads
  private final Collection<Cache<TableRuntimeContext, TableReadContext>> toBeInvalidatedThreadCaches;
  private ScheduledExecutorService executorServiceForTableSpooler;

  private HikariDataSource hikariDataSource;
  private ConnectionManager connectionManager;
  private Map<String, String> offsets;
  private ExecutorService executorService;
  private MultithreadedTableProvider tableOrderProvider;
  private int numberOfThreads;

  public AbstractTableJdbcSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean) {
    this.hikariConfigBean = hikariConfigBean;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.tableJdbcConfigBean = tableJdbcConfigBean;
    allTableContexts = new LinkedHashMap<>();
    qualifiedTableNameToConfigIndex = new HashMap<>();
    toBeInvalidatedThreadCaches = new ArrayList<>();
  }

  @Override
  protected List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    PushSource.Context context = getContext();
    issues = hikariConfigBean.validateConfigs(context, issues);
    issues = commonSourceConfigBean.validateConfigs(context, issues);

    validateTableJdbcConfigBean(context, issues);

    if (issues.isEmpty()) {
      checkConnectionAndBootstrap(context, issues);
    }
    return issues;
  }

  protected void checkConnectionAndBootstrap(Stage.Context context, List<ConfigIssue> issues) {
    try {
      hikariDataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
    } catch (StageException e) {
      issues.add(context.createConfigIssue(com.streamsets.pipeline.stage.origin.jdbc.table.Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
    }
    if (issues.isEmpty()) {
      try {
        connectionManager = new ConnectionManager(hikariDataSource);

        getTables(getContext(), issues, connectionManager);

      } catch (SQLException e) {
        JdbcUtil.logError(e);
        issues.add(context.createConfigIssue(com.streamsets.pipeline.stage.origin.jdbc.table.Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      } catch (StageException e) {
        LOG.debug("Error when finding tables:", e);
        issues.add(
            context.createConfigIssue(
                com.streamsets.pipeline.stage.origin.jdbc.table.Groups.TABLE.name(),
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

  private Map<Integer, Integer> decideMaxTableSlotsForThreads() {
    Map<Integer, Integer> threadNumberToMaxQueueSize = new HashMap<>();
    if (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      //If it is switch table strategy, we equal divide the work between all threads
      //(and if it cannot be equal distribute the remaining table slots to subset of threads)
      int totalNumberOfTables = allTableContexts.size();
      int balancedQueueSize = totalNumberOfTables / numberOfThreads;
      //first divide total tables / number of threads to get
      //an exact balanced number of table slots to be assigned to all threads
      IntStream.range(0, numberOfThreads).forEach(
          threadNumber -> threadNumberToMaxQueueSize.put(threadNumber, balancedQueueSize)
      );
      //Remaining table slots which are not assigned, can be assigned to a subset of threads
      int toBeAssignedTableSlots = totalNumberOfTables % numberOfThreads;

      //Randomize threads and pick a set of threads for processing extra slots
      List<Integer> threadNumbers = IntStream.range(0, numberOfThreads).boxed().collect(Collectors.toList());
      Collections.shuffle(threadNumbers);
      threadNumbers = threadNumbers.subList(0, toBeAssignedTableSlots);

      //Assign the remaining table slots to thread by incrementing the max table slot for each of the randomly selected
      //thread by 1
      for (int threadNumber : threadNumbers) {
        threadNumberToMaxQueueSize.put(threadNumber, threadNumberToMaxQueueSize.get(threadNumber) + 1);
      }
    } else {
      //Assign one table slot to each thread if the strategy is process all available rows
      //So each table will pick up one table process it completely then return it back to pool
      //then pick up a new table and work on it.
      IntStream.range(0, numberOfThreads).forEach(
          threadNumber -> threadNumberToMaxQueueSize.put(threadNumber, 1)
      );
    }
    return threadNumberToMaxQueueSize;
  }

  /**
   *
   * @param context the stage context
   * @param issues
   * @param allTableContexts
   * @param qualifiedTableNameToTableConfigIndex the map from qualified table names to corresponding index of the
   * {@link TableConfigBean} that leds  Once API-138 is complete, the index here can be consulted in order to set the precise list index of the
   * {@link TableConfigBean} that resulted in the {@link TableContext} having the issue
   * @return
   */
  private List<ConfigIssue> validatePartitioningConfigs(
      Stage.Context context,
      List<ConfigIssue> issues,
      Map<String, TableContext> allTableContexts,
      Map<String, Integer> qualifiedTableNameToTableConfigIndex
  ) {
    for (Map.Entry<String, TableContext> tableEntry : allTableContexts.entrySet()) {
      TableContext table = tableEntry.getValue();
      final String tableName = table.getQualifiedName();

      if (table.getPartitioningMode() == PartitioningMode.REQUIRED && !table.isPartitionable()) {
        List<String> reasons = new LinkedList<>();
        TableContext.isPartitionable(table, reasons);
        final ConfigIssue issue = context.createConfigIssue(
            com.streamsets.pipeline.stage.origin.jdbc.table.Groups.TABLE.name(),
            TableJdbcConfigBean.TABLE_CONFIG,
            JdbcErrors.JDBC_100,
            tableName,
            StringUtils.join(reasons, ", ")
        );

        if (qualifiedTableNameToTableConfigIndex.containsKey(tableName)) {
          // TODO: once API-138 is complete, do this (also for other issues created in this method)
          // issue.setAdditionalInfo("index", qualifiedTableNameToTableConfigIndex.get(tableName));
        }
        issues.add(issue);
      }

      if (table.getPartitioningMode() != PartitioningMode.DISABLED && table.isPartitionable()) {
        Map.Entry<String, Integer> entry = table.getOffsetColumnToType().entrySet().iterator().next();

        String partitionSize = table.getOffsetColumnToPartitionOffsetAdjustments().get(entry.getKey());

        final int maxActivePartitions = table.getMaxNumActivePartitions();
        if (maxActivePartitions == 0 || maxActivePartitions == 1) {
          // TODO: set index once API-138 is complete
          issues.add(context.createConfigIssue(
              com.streamsets.pipeline.stage.origin.jdbc.table.Groups.TABLE.name(),
              TableJdbcConfigBean.TABLE_CONFIG,
              JdbcErrors.JDBC_102,
              maxActivePartitions,
              tableName
          ));
        }

        final String validationError = TableContextUtil.getPartitionSizeValidationError(
            entry.getValue(),
            entry.getKey(),
            partitionSize
        );
        if (!Strings.isNullOrEmpty(validationError)) {
          // TODO: set index once API-138 is complete
          issues.add(context.createConfigIssue(
              com.streamsets.pipeline.stage.origin.jdbc.table.Groups.TABLE.name(),
              TableJdbcConfigBean.TABLE_CONFIG,
              JdbcErrors.JDBC_101,
              tableName,
              validationError
          ));
        }
      }
    }


    return issues;
  }

  private void getTables(Stage.Context context, List<ConfigIssue> issues, ConnectionManager connectionManager) throws StageException, SQLException {
    // clear the list
    allTableContexts.clear();

    allTableContexts = listTablesForConfig(getContext(), issues, connectionManager);

    LOG.info("Selected Tables: \n {}", NEW_LINE_JOINER.join(allTableContexts.keySet()));

    if (allTableContexts.isEmpty() && !commonSourceConfigBean.allowLateTable) {
      issues.add(context.createConfigIssue(com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.Groups.TABLE.name(), TableJdbcConfigBean.TABLE_CONFIG, JdbcErrors.JDBC_66));
    } else {
      issues = validatePartitioningConfigs(context, issues, allTableContexts, qualifiedTableNameToConfigIndex);
      if (!issues.isEmpty()) {
        return;
      }

      numberOfThreads = tableJdbcConfigBean.numberOfThreads;

      TableOrderProvider tableOrderProvider = new TableOrderProviderFactory(connectionManager.getConnection(),
          tableJdbcConfigBean.tableOrderStrategy
      ).create();

      try {
        tableOrderProvider.initialize(allTableContexts);

        if (this.tableOrderProvider == null) {
          this.tableOrderProvider = new MultithreadedTableProvider(
              allTableContexts,
              tableOrderProvider.getOrderedTables(),
              decideMaxTableSlotsForThreads(),
              numberOfThreads,
              tableJdbcConfigBean.batchTableStrategy
          );
        } else {
          this.tableOrderProvider.setTableContextMap(allTableContexts, tableOrderProvider.getOrderedTables());
        }
      } catch (ExecutionException e) {
        LOG.error("Error during Table Order Provider Init", e);
        throw new StageException(JdbcErrors.JDBC_67, e);
      }

      //Accessed by all runner threads
      offsets = new ConcurrentHashMap<>();
    }
  }

  @Override
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    int batchSize = Math.min(maxBatchSize, commonSourceConfigBean.maxBatchSize);
    handleLastOffset(new HashMap<>(lastOffsets));
    try {
      executorService = new SafeScheduledExecutorService(numberOfThreads, TableJdbcRunnable.TABLE_JDBC_THREAD_PREFIX);

      ExecutorCompletionService<Future> completionService = new ExecutorCompletionService<>(executorService);

      final RateLimiter queryRateLimiter = commonSourceConfigBean.creatQueryRateLimiter();

      List<Future> allFutures = new LinkedList<>();
      IntStream.range(0, numberOfThreads).forEach(threadNumber -> {
        JdbcBaseRunnable runnable = new JdbcRunnableBuilder()
            .context(getContext())
            .threadNumber(threadNumber)
            .batchSize(batchSize)
            .connectionManager(connectionManager)
            .offsets(offsets)
            .tableProvider(tableOrderProvider)
            .tableReadContextCache(getTableReadContextCache(connectionManager, offsets))
            .commonSourceConfigBean(commonSourceConfigBean)
            .tableJdbcConfigBean(tableJdbcConfigBean)
            .queryRateLimiter(commonSourceConfigBean.creatQueryRateLimiter())
            .build();

        toBeInvalidatedThreadCaches.add(runnable.getTableReadContextCache());
        allFutures.add(completionService.submit(runnable, null));
      });

      if (commonSourceConfigBean.allowLateTable) {
        TableSpooler tableSpooler = new TableSpooler();
        executorServiceForTableSpooler = new SafeScheduledExecutorService(1, JdbcBaseRunnable.TABLE_JDBC_THREAD_PREFIX);
        executorServiceForTableSpooler.scheduleWithFixedDelay(
            tableSpooler,
            0,
            commonSourceConfigBean.newTableQueryInterval,
            TimeUnit.SECONDS
        );
      }

      while (!getContext().isStopped()) {
        checkWorkerStatus(completionService);
        JdbcUtil.generateNoMoreDataEventIfNeeded(tableOrderProvider.shouldGenerateNoMoreDataEvent(), getContext());
      }

      for (Future future : allFutures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          LOG.error(
              "ExecutionException when attempting to wait for all table JDBC runnables to complete, after context was" +
                  " stopped: {}",
              e.getMessage(),
              e
          );
        } catch (InterruptedException e) {
          LOG.error(
              "InterruptedException when attempting to wait for all table JDBC runnables to complete, after context " +
                  "was stopped: {}",
              e.getMessage(),
              e
          );
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      if (shutdownExecutorIfNeeded()) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Checks whether any of the {@link TableJdbcRunnable} workers completed
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

  @Override
  public void destroy() {
    boolean interrupted = shutdownExecutorIfNeeded();
    //Invalidate all the thread cache so that all statements/result sets are properly closed.
    toBeInvalidatedThreadCaches.forEach(Cache::invalidateAll);
    //Closes all connections
    Optional.ofNullable(connectionManager).ifPresent(ConnectionManager::closeAll);
    JdbcUtil.closeQuietly(hikariDataSource);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private synchronized boolean shutdownExecutorIfNeeded() {
    AtomicBoolean interrupted = new AtomicBoolean(false);
    Optional.ofNullable(executorService).ifPresent(executor -> {
      if (!executor.isTerminated()) {
        LOG.info("Shutting down executor service");
        executor.shutdown();
        try {
          executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Shutdown interrupted");
          interrupted.set(true);
        }
      }
    });
    return interrupted.get();
  }

  /**
   * Running the another thread to discover the target tables
   */
  class TableSpooler implements Runnable {
    public void run() {
      Thread.currentThread().setName("JDBC Multithread Fetch Tables");

      try {
        getTables(getContext(), new ArrayList<>(), connectionManager);
      } catch (StageException | SQLException e) {
        LOG.error("Exception thrown during fetching tables from metada", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected Map<String, String> getOffsets() {
    return offsets;
  }

  protected Map<String, TableContext> getAllTableContexts() {
    return allTableContexts;
  }

  protected MultithreadedTableProvider getTableOrderProvider() {
    return tableOrderProvider;
  }

  protected abstract void handleLastOffset(Map<String, String> lastOffsets) throws StageException;

  protected abstract void validateTableJdbcConfigBean(PushSource.Context context, List<Stage.ConfigIssue> issues);

  protected abstract Map<String, TableContext> listTablesForConfig(
      PushSource.Context context,
      List<ConfigIssue> issues,
      ConnectionManager connectionManager
  ) throws SQLException, StageException;

  protected abstract CacheLoader<TableRuntimeContext, TableReadContext> getTableReadContextCache(
      ConnectionManager connectionManager,
      Map<String, String> offsets
  );
}
