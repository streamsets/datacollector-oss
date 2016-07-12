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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

/**
 * Executor (destination) that executes given queries against hive or impala.
 */
public class HiveQueryExecutor extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryExecutor.class);

  private HiveQueryExecutorConfig config;
  private ErrorRecordHandler errorRecordHandler;

  public HiveQueryExecutor(HiveQueryExecutorConfig config) {
    this.config = config;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    config.init(getContext(), "config.hiveConfigBean", issues);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return  issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    ELVars variables = getContext().createELVars();
    ELEval eval = getContext().createELEval("query");

    Iterator<Record> it = batch.getRecords();
    while(it.hasNext()) {
      Record record = it.next();
      RecordEL.setRecordInContext(variables, record);

      String query = eval.eval(variables, config.query, String.class);
      LOG.info("Executing query: {}", query);

      try(Statement stmt = config.hiveConfigBean.getHiveConnection().createStatement()) {
        stmt.execute(query);
      } catch(SQLException ex) {
        LOG.error("Can't execute query", ex);
        errorRecordHandler.onError(new OnRecordErrorException(record, QueryExecErrors.QUERY_EXECUTOR_001, query));
      }
    }
  }

  @Override
  public void destroy() {
    config.destroy();
    super.destroy();
  }
}
