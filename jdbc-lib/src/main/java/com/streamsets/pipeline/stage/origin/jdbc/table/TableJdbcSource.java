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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
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
import com.streamsets.pipeline.lib.util.OffsetUtil;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableJdbcSource extends BasePushSource {
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$";
  public static final String OFFSET_VERSION_1 = "1";
  public static final String OFFSET_VERSION_2 = "2";

  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSource.class);
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private final HikariPoolConfigBean hikariConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final Map<String, TableContext> allTableContexts;
  private final Map<String, Integer> qualifiedTableNameToConfigIndex;
  //If we have more state to clean up, we can introduce a state manager to do that which
  //can keep track of different closeables from different threads
  private final Collection<Cache<TableRuntimeContext, TableReadContext>> toBeInvalidatedThreadCaches;

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
    allTableContexts = new LinkedHashMap<>();
    qualifiedTableNameToConfigIndex = new HashMap<>();
    toBeInvalidatedThreadCaches = new ArrayList<>();
  }

  @VisibleForTesting
  void checkConnectionAndBootstrap(Stage.Context context, List<ConfigIssue> issues) {
    try {
      hikariDataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
    } catch (StageException e) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
    }
    if (issues.isEmpty()) {
      try {
        connectionManager = new ConnectionManager(hikariDataSource);
        for (int i = 0; i < tableJdbcConfigBean.tableConfigs.size(); i++) {
          TableConfigBean tableConfigBean = tableJdbcConfigBean.tableConfigs.get(i);

          //No duplicates even though a table matches multiple configurations, we will add it only once.
          final Map<String, TableContext> tableContexts =
              TableContextUtil.listTablesForConfig(
                  getContext(),
                  issues,
                  connectionManager.getConnection(),
                  tableConfigBean,
                  new TableJdbcELEvalContext(context, context.createELVars()),
                  tableJdbcConfigBean.quoteChar

          );

          allTableContexts.putAll(tableContexts);
          for (String qualifiedTableName : tableContexts.keySet()) {
            qualifiedTableNameToConfigIndex.put(qualifiedTableName, i);
          }
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
          issues = validatePartitioningConfigs(context, issues, allTableContexts, qualifiedTableNameToConfigIndex);
          if (!issues.isEmpty()) {
            return;
          }

          numberOfThreads = tableJdbcConfigBean.numberOfThreads;

          TableOrderProvider tableOrderProvider = new TableOrderProviderFactory(
              connectionManager.getConnection(),
              tableJdbcConfigBean.tableOrderStrategy
          ).create();

          try {
            tableOrderProvider.initialize(allTableContexts);
            this.tableOrderProvider = new MultithreadedTableProvider(
                allTableContexts,
                tableOrderProvider.getOrderedTables(),
                decideMaxTableSlotsForThreads(),
                numberOfThreads,
                tableJdbcConfigBean.batchTableStrategy
            );
          } catch (ExecutionException e) {
            LOG.debug("Error during Table Order Provider Init", e);
            throw new StageException(JdbcErrors.JDBC_67, e);
          }
          //Accessed by all runner threads
          offsets = new ConcurrentHashMap<>();
        }
      } catch (SQLException e) {
        JdbcUtil.logError(e);
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

  @Override
  protected List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    PushSource.Context context = getContext();
    issues = hikariConfigBean.validateConfigs(context, issues);
    issues = commonSourceConfigBean.validateConfigs(context, issues);
    issues = tableJdbcConfigBean.validateConfigs(context, issues);

    //Max pool size should be equal to number of threads
    //The main thread will use one connection to list threads and close (return to hikari pool) the connection
    // and each individual data threads needs one connection
    if (tableJdbcConfigBean.numberOfThreads > hikariConfigBean.maximumPoolSize) {
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
            Groups.TABLE.name(),
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
              Groups.TABLE.name(),
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
              Groups.TABLE.name(),
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

      List<Future> allFutures = new LinkedList<>();
      IntStream.range(0, numberOfThreads).forEach(threadNumber -> {
        JdbcBaseRunnable runnable = new JdbcRunnableBuilder()
            .context(getContext())
            .threadNumber(threadNumber)
            .batchSize(batchSize)
            .connectionManager(connectionManager)
            .offsets(offsets)
            .tableProvider(tableOrderProvider)
            .commonSourceConfigBean(commonSourceConfigBean)
            .tableJdbcConfigBean(tableJdbcConfigBean)
            .build();
        toBeInvalidatedThreadCaches.add(runnable.getTableReadContextCache());
        allFutures.add(completionService.submit(runnable, null));
      });

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
      shutdownExecutorIfNeeded();
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

  @VisibleForTesting
  void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets != null) {
      String offsetVersion = null;
      //Only if it is not already committed
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

        upgradeFromV1();

        //Remove Poll Source Offset key from the offset.
        //Do this at last so as not to lose the offsets if there is failure in the middle
        //when we call commitOffset above
        getContext().commitOffset(Source.POLL_SOURCE_OFFSET_KEY, null);
      } else {
        offsetVersion = lastOffsets.remove(OFFSET_VERSION);

        if (OFFSET_VERSION_2.equals(offsetVersion)) {
          final Map<String, String> newCommitOffsets = new HashMap<>();
          tableOrderProvider.initializeFromV2Offsets(lastOffsets, newCommitOffsets);

          //clear out existing offset keys and recommit new ones
          for (String offsetKey : lastOffsets.keySet()) {
            if (OFFSET_VERSION.equals(offsetKey)) {
              continue;
            }
            getContext().commitOffset(offsetKey, null);
          }
          offsets.putAll(newCommitOffsets);
          newCommitOffsets.forEach((key, value) -> getContext().commitOffset(key, value));
        } else if (OFFSET_VERSION_1.equals(offsetVersion) || Strings.isNullOrEmpty(offsetVersion)) {
          offsets.putAll(lastOffsets);
          upgradeFromV1();
        }
      }


      //Version the offset so as to allow for future evolution.
      getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_2);

    }
  }

  private void upgradeFromV1() throws StageException {
    final Set<String> offsetKeysToRemove = tableOrderProvider.initializeFromV1Offsets(offsets);
    if (offsetKeysToRemove != null && offsetKeysToRemove.size() > 0) {
      LOG.info("Removing now outdated offset keys: {}", offsetKeysToRemove);
      offsetKeysToRemove.forEach(tableName -> getContext().commitOffset(tableName, null));
    }
  }
}
