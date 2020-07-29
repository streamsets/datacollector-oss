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
package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JdbcQueryExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcQueryExecutor.class);

  private static final String QUERIES_CONFIGURATION = "config.queries";

  private static final String INSERT = "INSERT";
  private static final String DELETE = "DELETE";

  private JdbcQueryExecutorConfig config;
  private ErrorRecordHandler errorRecordHandler;

  private ExecutorService service;

  public JdbcQueryExecutor(JdbcQueryExecutorConfig config) {
    this.config = config;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    config.init(getContext(), issues);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    validateQueries(issues);

    try (Connection connection = config.getConnection()) {
      if (connection == null) {
        issues.add(getContext().createConfigIssue(com.streamsets.pipeline.stage.executor.jdbc.Groups.JDBC.name(),
            QUERIES_CONFIGURATION,
            QueryExecErrors.QUERY_EXECUTOR_002
        ));
      }
    } catch (SQLException e) {
      issues.add(getContext().createConfigIssue(com.streamsets.pipeline.stage.executor.jdbc.Groups.JDBC.name(),
          QUERIES_CONFIGURATION,
          QueryExecErrors.QUERY_EXECUTOR_002,
          e.getMessage()
      ));
    }

    if (config.isParallel()) {
      if (config.getHikariConfigBean().maximumPoolSize != config.getHikariConfigBean().minIdle) {
        LOG.error(QueryExecErrors.QUERY_EXECUTOR_003.getMessage());
        issues.add(getContext().createConfigIssue(Groups.ADVANCED.name(),
            "config.isParallel",
            QueryExecErrors.QUERY_EXECUTOR_003
        ));
      } else {
        service = Executors.newFixedThreadPool(config.getHikariConfigBean().maximumPoolSize);
      }
    }
    return issues;
  }

  private void validateQueries(List<ConfigIssue> issues) {
    List<String> queries = config.getQueries();

    boolean queriesAreEmpty = true;
    for (String query : queries) {
      if (!query.trim().isEmpty()) {
        queriesAreEmpty = false;
        break;
      }
    }
    if (queriesAreEmpty) {
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(),
          QUERIES_CONFIGURATION,
          QueryExecErrors.QUERY_EXECUTOR_007
      ));
    }

  }

  @Override
  public void write(Batch batch) {
    if (config.isParallel()) {
      processInParallel(errorRecordHandler, batch);
    } else {
      processSerially(errorRecordHandler, batch);
    }
  }

  private void processSerially(ErrorRecordHandler errorRecordHandler, Batch batch) {
    ELVars variables = getContext().createELVars();
    ELEval eval = getContext().createELEval(config.getQueriesVariableName());

    Iterator<Record> it = batch.getRecords();
    try (Connection connection = config.getConnection()) {
      while (it.hasNext()) {
        Record record = it.next();
        RecordEL.setRecordInContext(variables, record);

        for (String query : config.getQueries()) {
          if (!query.trim().isEmpty()) {
            processARecord(connection, errorRecordHandler, eval.eval(variables, query, String.class), record);
          }
        }
      }

      if (config.isBatchCommit()) {
        connection.commit();
      }

    } catch (SQLException ex) {
      LOG.error("Can't get connection", ex);
      throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, ex.getMessage());
    }

  }

  private void processInParallel(ErrorRecordHandler errorRecordHandler, Batch batch) {
    ELVars variables = getContext().createELVars();
    ELEval eval = getContext().createELEval(config.getQueriesVariableName());

    Iterator<Record> it = batch.getRecords();

    List<Future<Void>> insertsAndDeletes = new LinkedList<>();
    Set<Map.Entry<String, Record>> updatesAndOthers = new LinkedHashSet<>();
    Set<Map.Entry<String, Record>> deletes = new LinkedHashSet<>();
    DBTask task;

    while (it.hasNext()) {
      Record record = it.next();
      RecordEL.setRecordInContext(variables, record);

      for (String rawQuery : config.getQueries()) {
        if (!rawQuery.trim().isEmpty()) {
          String query = eval.eval(variables, rawQuery, String.class).trim();
          query = query.replace(query.substring(0, query.indexOf(" ")),
              query.substring(0, query.indexOf(" ")).toUpperCase());

          if (query.startsWith(INSERT)) {
            insertsAndDeletes.add(service.submit(new DBTask(
                new AbstractMap.SimpleEntry<>(query, record),
                errorRecordHandler,
                config
            )));
          } else if (query.startsWith(DELETE)) {
            deletes.add(new AbstractMap.SimpleEntry<>(query, record));
          } else {
            updatesAndOthers.add(new AbstractMap.SimpleEntry<>(query, record));
          }
        }
      }
    }

    // wait for the INSERTS to complete.
    waitForCompletion(insertsAndDeletes);

    // process UPDATE and other statements.
    // we serialize all the statements from this list.
    // this is particularly important for UPDATE statements, as
    // there is a very remote (but possible) chance
    // that 2 back-to-back UPDATEs for the same record
    // could execute out-of-order.
    if (!updatesAndOthers.isEmpty()) {
      try (Connection connection = config.getConnection()) {
        for (Map.Entry<String, Record> element : updatesAndOthers) {
          processARecord(connection, errorRecordHandler, element.getKey(), element.getValue());
        }
        if (!config.getHikariConfigBean().isAutoCommit()) {
          connection.commit();
        }

      } catch (SQLException ex) {
        LOG.error("Can't get connection", ex);
        throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, ex.getMessage());
      }
    }

    // finally, process DELETE statements in parallel.
    if (!deletes.isEmpty()) {
      insertsAndDeletes.clear();
      for (Map.Entry<String, Record> element : deletes) {
        task = new DBTask(element, errorRecordHandler, config);
        insertsAndDeletes.add(service.submit(task));
      }
      waitForCompletion(insertsAndDeletes);
    }
  }

  private void processARecord(
      Connection connection, ErrorRecordHandler errorRecordHandler, String query, Record record
  ) {
    LOG.trace("Executing query: {}", query);
    ResultSet rs = null;

    try (Statement stmt = connection.createStatement()) {
      final boolean queryResult = stmt.execute(query);

      if (queryResult && config.queryResultCount) {
        int count = 0;
        rs = stmt.getResultSet();
        while (rs.next()) {
          count++;
        }

        JdbcQueryExecutorEvents.successfulQuery.create(getContext()).with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD,
            query
        ).with(JdbcQueryExecutorEvents.QUERY_RESULT_FIELD, count + " row(s) returned").createAndSend();
      } else if (config.queryResultCount) {
        JdbcQueryExecutorEvents.successfulQuery.create(getContext()).with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD,
            query
        ).with(JdbcQueryExecutorEvents.QUERY_RESULT_FIELD, stmt.getUpdateCount() + " row(s) affected").createAndSend();
      } else {
        JdbcQueryExecutorEvents.successfulQuery.create(getContext()).with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD,
            query
        ).createAndSend();
      }
    } catch (SQLException ex) {
      LOG.error("Can't execute query", ex);

      EventCreator.EventBuilder failedQueryEventBuilder =
          JdbcQueryExecutorEvents.failedQuery.create(getContext()).with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD,
          query
      );
      errorRecordHandler.onError(new OnRecordErrorException(record,
          QueryExecErrors.QUERY_EXECUTOR_001,
          query,
          ex.getMessage()
      ));

      failedQueryEventBuilder.createAndSend();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        LOG.trace("Failed to close result set", e);
      }
    }
  }

  private void waitForCompletion(List<Future<Void>> insertsAndDeletes) {
    try {
      for (Future<Void> element : insertsAndDeletes) {
        element.get();
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.info("InterruptedException: {}", ex.getMessage(), ex);
      throw new StageException(QueryExecErrors.QUERY_EXECUTOR_006, ex.toString(), ex);
    }
  }

  @Override
  public void destroy() {
    config.destroy();

    // will be null if not in "parallel" mode.
    if (service != null) {
      service.shutdown();
      service = null;    //may change mode between runs.
    }

    super.destroy();
  }
}
