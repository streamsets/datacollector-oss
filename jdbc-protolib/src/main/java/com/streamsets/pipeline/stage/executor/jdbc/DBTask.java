package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

class DBTask implements Callable<Void> {
  private final Logger LOG = LoggerFactory.getLogger(DBTask.class);
  private WorkQueueElement work;
  private ErrorRecordHandler errorRecordHandler;
  private JdbcQueryExecutorConfig config;

  public DBTask(WorkQueueElement work, ErrorRecordHandler errorRecordHandler, JdbcQueryExecutorConfig config) {
    LOG.trace("DBTask: create thread {} query {} ", Thread.currentThread().getName(), work.query);
    this.work = work;
    this.errorRecordHandler = errorRecordHandler;
    this.config = config;
  }

  @Override
  public Void call() throws StageException {
    LOG.debug("thread {} query {} ", Thread.currentThread().getName(), work.query);

    try (Connection conn = config.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(work.query);
        if(!config.hikariConfigBean.autoCommit) {
          conn.commit();
        }
      } catch (SQLException ex) {
        // DEBUG level due to the fact that duplicate keys, etc kick out here.
        LOG.trace(QueryExecErrors.QUERY_EXECUTOR_004.getMessage(), ex.getMessage());
        synchronized (errorRecordHandler) {
          errorRecordHandler.onError(new OnRecordErrorException(work.record,
              QueryExecErrors.QUERY_EXECUTOR_004,
              work.query,
              ex.getMessage()
          ));
        }
      }
    } catch (SQLException ex) {
      LOG.error(QueryExecErrors.QUERY_EXECUTOR_005.getMessage(), ex.getMessage(), ex);
      synchronized (errorRecordHandler) {
        errorRecordHandler.onError(new OnRecordErrorException(work.record,
            QueryExecErrors.QUERY_EXECUTOR_005,
            work.query,
            ex.getMessage()
        ));
      }
    }
    return null;
  }
}
