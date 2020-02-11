/*
 * Copyright 2019 StreamSets Inc.
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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.Callable;

class DBTask implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(DBTask.class);
  private final String query;
  private final Record record;
  private final ErrorRecordHandler errorRecordHandler;
  private final JdbcQueryExecutorConfig config;

  public DBTask(
      Map.Entry<String, Record> queryAndRecord, ErrorRecordHandler errorRecordHandler, JdbcQueryExecutorConfig config
  ) {
    LOG.trace("DBTask: create thread {} query {} ", Thread.currentThread().getName(), queryAndRecord.getKey());
    this.query = queryAndRecord.getKey();
    this.record = queryAndRecord.getValue();
    this.errorRecordHandler = errorRecordHandler;
    this.config = config;
  }

  @Override
  public Void call() {
    LOG.debug("thread {} query {} ", Thread.currentThread().getName(), query);

    try (Connection conn = config.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(query);
        if (!config.getHikariConfigBean().isAutoCommit()) {
          conn.commit();
        }
      } catch (SQLException ex) {
        // DEBUG level due to the fact that duplicate keys, etc kick out here.
        LOG.trace(QueryExecErrors.QUERY_EXECUTOR_004.getMessage(), ex.getMessage());
        synchronized (errorRecordHandler) {
          errorRecordHandler.onError(new OnRecordErrorException(record,
              QueryExecErrors.QUERY_EXECUTOR_004,
              query,
              ex.getMessage()
          ));
        }
      }
    } catch (SQLException ex) {
      LOG.error(QueryExecErrors.QUERY_EXECUTOR_005.getMessage(), ex.getMessage(), ex);
      synchronized (errorRecordHandler) {
        errorRecordHandler.onError(new OnRecordErrorException(record,
            QueryExecErrors.QUERY_EXECUTOR_005,
            query,
            ex.getMessage()
        ));
      }
    }
    return null;
  }
}
