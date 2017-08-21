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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Executor (destination) that executes given queries against hive or impala.
 */
public class HiveQueryExecutor extends BaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryExecutor.class);

  private HiveQueryExecutorConfig config;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval queriesElEval;

  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");

  public HiveQueryExecutor(HiveQueryExecutorConfig config) {
    this.config = config;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // Validate that queries are valid ELs
    for(String query : config.queries) {
      try {
        getContext().parseEL(query);
      } catch (ELEvalException e) {
        issues.add(getContext().createConfigIssue(
          Groups.QUERY.name(),
          "config.queries",
          QueryExecErrors.QUERY_EXECUTOR_002,
          e.getMessage(),
          e
          ));
      }
    }

    config.init(getContext(), "config.hiveConfigBean", issues);
    queriesElEval = getContext().createELEval("queries");
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return  issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    ELVars variables = getContext().createELVars();
    while(it.hasNext()) {
      Record record = it.next();
      RecordEL.setRecordInContext(variables, record);
      try {
        List<String> queriesToExecute = getEvaluatedQueriesForTheRecord(record, variables);
        executeQueries(record, queriesToExecute);
      } catch (OnRecordErrorException e) {
        LOG.error("Error when processing record: {}", e.toString(), e);
        errorRecordHandler.onError(e);
      }
    }
  }

  private List<String> getEvaluatedQueriesForTheRecord(Record record, ELVars variables) throws OnRecordErrorException {
    List<String> evaluatedQueries = new ArrayList<>();
    List<String> badQueriesAndErrors = new ArrayList<>();
    for (String confQuery : config.queries) {
      try {
        String query = queriesElEval.eval(variables, confQuery, String.class);
        evaluatedQueries.add(query);
      } catch (ELEvalException e) {
        LOG.error("Error evaluating Query : {}, Reason: {}", confQuery, e);
        badQueriesAndErrors.add(Utils.format("Query Failed to Evaluate: {}. Error: {}", confQuery, e));
      }
    }
    if (!badQueriesAndErrors.isEmpty()) {
      throw new OnRecordErrorException(record, QueryExecErrors.QUERY_EXECUTOR_002, NEW_LINE_JOINER.join(badQueriesAndErrors));
    }
    return evaluatedQueries;
  }

  private void executeQueries(Record record, List<String> queriesToExecute) throws StageException {
    List<Object> remainingQueriesToExecute = new ArrayList<>();
    remainingQueriesToExecute.addAll(queriesToExecute);

    List<String> failedQueriesToReason = new ArrayList<>();
    for (String query : queriesToExecute) {
      //Remove queries from queries to execute as we are going to execute
      remainingQueriesToExecute.remove(query);
      //Get and validate connection before each query execution
      //to make sure we don't have stale connection
      try (Statement st = config.hiveConfigBean.getHiveConnection().createStatement()){
        LOG.debug("Executing Query {}", query);
        st.execute(query);
        //Query Successful
        HiveQueryExecutorEvents.successfulQuery.create(getContext()).with(HiveQueryExecutorEvents.QUERY_EVENT_FIELD, query).createAndSend();
      } catch (SQLException e) {
        LOG.error("Failed to execute query: {}", query, e);
        //Query failed for some reason
        EventCreator.EventBuilder failedQueryEventBuilder = HiveQueryExecutorEvents.failedQuery.create(getContext()).with(HiveQueryExecutorEvents.QUERY_EVENT_FIELD, query);
        failedQueriesToReason.add(Utils.format("Failed to execute query '{}'. Reason: {}", query, e));

        //Optionally populate unexecutedQueries
        if (config.stopOnQueryFailure && !remainingQueriesToExecute.isEmpty()) {
          failedQueryEventBuilder.withStringList(HiveQueryExecutorEvents.UNEXECUTED_QUERIES_EVENT_FIELD, remainingQueriesToExecute);
        }

        failedQueryEventBuilder.createAndSend();

        if (config.stopOnQueryFailure) {
          break;
        }
      }
    }
    //Join all the error queries and reason with a new line joiner and use that in the error.
    if (!failedQueriesToReason.isEmpty()) {
      throw new OnRecordErrorException(record, QueryExecErrors.QUERY_EXECUTOR_001, NEW_LINE_JOINER.join(failedQueriesToReason));
    }
  }

  @Override
  public void destroy() {
    config.destroy();
    super.destroy();
  }
}
