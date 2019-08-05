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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JdbcQueryExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcQueryExecutor.class);

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
    if (config.parallel) {
      if (config.hikariConfigBean.maximumPoolSize != config.hikariConfigBean.minIdle) {
        LOG.error(QueryExecErrors.QUERY_EXECUTOR_003.getMessage());
        issues.add(getContext().createConfigIssue(
            Groups.ADVANCED.name(),
            "parallel",
            QueryExecErrors.QUERY_EXECUTOR_003
        ));
      } else {
        service = Executors.newFixedThreadPool(config.hikariConfigBean.maximumPoolSize);
      }
    }
    return  issues;
  }

  @Override
  public void write(Batch batch) throws StageException {

    if (config.parallel) {
      processInParallel(errorRecordHandler, batch);
    } else {
      processSerially(errorRecordHandler, batch);
    }
  }

  private void processSerially(ErrorRecordHandler errorRecordHandler, Batch batch) throws StageException {
    ELVars variables = getContext().createELVars();
    ELEval eval = getContext().createELEval("query");

    Iterator<Record> it = batch.getRecords();
    try (Connection connection = config.getConnection()) {
      while (it.hasNext()) {
        Record record = it.next();
        RecordEL.setRecordInContext(variables, record);
        String query = eval.eval(variables, config.query, String.class);

        processARecord(connection, errorRecordHandler, query, record);
      }

      if (config.batchCommit) {
        connection.commit();
      }

    } catch (SQLException ex) {
      LOG.error("Can't get connection", ex);
      throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, ex.getMessage());
    }

  }

  private void processInParallel(ErrorRecordHandler errorRecordHandler, Batch batch) throws StageException {
    ELVars variables = getContext().createELVars();
    ELEval eval = getContext().createELEval("query");

    Iterator<Record> it = batch.getRecords();

    List<Future<Void>> insertsAndDeletes = new LinkedList<>();
    Set<WorkQueueElement> updatesAndOthers = new LinkedHashSet<>();
    Set<WorkQueueElement> deletes = new LinkedHashSet<>();
    DBTask task;

    while (it.hasNext()) {
      Record record = it.next();
      RecordEL.setRecordInContext(variables, record);
      String query = eval.eval(variables, config.query, String.class);

      // TODO: split into INSERT/UPDATE (and others)/DELETE.
      query = query.trim();
      String cmd = query.substring(0, query.indexOf(" ")).trim().toUpperCase();

      if (cmd.equals("INSERT")) {
        insertsAndDeletes.add(service.submit(new DBTask(new WorkQueueElement(query, record), errorRecordHandler, config)));

      } else if (cmd.equals("DELETE")) {
        deletes.add(new WorkQueueElement(query, record));

      } else {
        updatesAndOthers.add(new WorkQueueElement(query, record));

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
    if (updatesAndOthers.size() > 0) {
      try (Connection connection = config.getConnection()) {
        for (WorkQueueElement e : updatesAndOthers) {
          processARecord(connection, errorRecordHandler, e.query, e.record);
        }
        if(!config.hikariConfigBean.autoCommit) {
          connection.commit();
        }

      } catch (SQLException ex) {
        LOG.error("Can't get connection", ex);
        throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, ex.getMessage());
      }
    }

    // finally, process DELETE statements in parallel.
    if (deletes.size() > 0) {
      insertsAndDeletes.clear();
      for (WorkQueueElement e : deletes) {
        task = new DBTask(e, errorRecordHandler, config);
        insertsAndDeletes.add(service.submit(task));
      }
      waitForCompletion(insertsAndDeletes);
    }
  }

  private void processARecord(Connection connection, ErrorRecordHandler errorRecordHandler, String query, Record record) throws StageException {
    LOG.trace("Executing query: {}", query);
    try (Statement stmt = connection.createStatement()) {
      final boolean queryResult = stmt.execute(query);

      if (queryResult && config.queryResultCount){
        int count = 0;
        ResultSet rs = stmt.getResultSet();
        while(rs.next()){
          count++;
        }

        JdbcQueryExecutorEvents.successfulQuery.create(getContext())
                .with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD, query)
                .with(JdbcQueryExecutorEvents.QUERY_RESULT_FIELD, count + " row(s) returned")
                .createAndSend();
      }
      else if (config.queryResultCount){
        JdbcQueryExecutorEvents.successfulQuery.create(getContext())
                .with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD, query)
                .with(JdbcQueryExecutorEvents.QUERY_RESULT_FIELD, stmt.getUpdateCount() + " row(s) affected")
                .createAndSend();
      }
      else{
        JdbcQueryExecutorEvents.successfulQuery.create(getContext())
                .with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD, query)
                .createAndSend();
      }
    } catch (SQLException ex) {
      LOG.error("Can't execute query", ex);

      EventCreator.EventBuilder failedQueryEventBuilder = JdbcQueryExecutorEvents.failedQuery.create(getContext()).with(JdbcQueryExecutorEvents.QUERY_EVENT_FIELD, query);
      errorRecordHandler.onError(new OnRecordErrorException(record,
          QueryExecErrors.QUERY_EXECUTOR_001,
          query,
          ex.getMessage()
      ));

      failedQueryEventBuilder.createAndSend();
    }
  }

  private void waitForCompletion(List<Future<Void>> insertsAndDeletes) throws StageException {
    try {
      for (Future<Void> f : insertsAndDeletes) {
        f.get();
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
