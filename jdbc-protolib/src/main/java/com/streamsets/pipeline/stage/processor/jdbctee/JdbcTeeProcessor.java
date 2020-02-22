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
package com.streamsets.pipeline.stage.processor.jdbctee;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JDBCOperationType;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcRecordReaderWriterFactory;
import com.streamsets.pipeline.lib.jdbc.JdbcRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JdbcTeeProcessor extends SingleLaneProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTeeProcessor.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String MULTI_ROW_OP = "useMultiRowOp";

  private final boolean rollbackOnError;
  private final boolean useMultiRowOp;
  private final int maxPrepStmtParameters;

  private final String schema;
  private final String tableNameTemplate;
  private final List<JdbcFieldColumnParamMapping> customMappings;
  private final List<JdbcFieldColumnMapping> generatedColumnMappings;
  private final boolean caseSensitive;
  private final boolean dynamicTableName;

  private final ChangeLogFormat changeLogFormat;
  private final HikariPoolConfigBean hikariConfigBean;
  private final CacheCleaner cacheCleaner;

  private final JdbcUtil jdbcUtil;

  private ELEval tableNameEval = null;
  private ELVars tableNameVars = null;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;

  private JDBCOperationType defaultOperation;
  private UnsupportedOperationAction unsupportedAction;

  public JdbcTeeProcessor(
      String schema,
      String tableNameTemplate,
      List<JdbcFieldColumnParamMapping> customMappings,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      boolean caseSensitive,
      boolean rollbackOnError,
      boolean useMultiRowOp,
      int maxPrepStmtParameters,
      ChangeLogFormat changeLogFormat,
      HikariPoolConfigBean hikariConfigBean,
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction
  ) {
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
    this.schema = schema;
    this.tableNameTemplate = tableNameTemplate;
    this.customMappings = customMappings;
    this.generatedColumnMappings = generatedColumnMappings;
    this.caseSensitive = caseSensitive;
    this.rollbackOnError = rollbackOnError;
    this.useMultiRowOp = useMultiRowOp;
    this.maxPrepStmtParameters = maxPrepStmtParameters;
    this.changeLogFormat = changeLogFormat;
    this.hikariConfigBean = hikariConfigBean;
    this.defaultOperation = defaultOp;
    this.unsupportedAction = unsupportedAction;
    this.dynamicTableName = this.jdbcUtil.isElString(tableNameTemplate);

    CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(500)
        .expireAfterAccess(1, TimeUnit.HOURS);

    if(LOG.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }

    this.recordWriters = cacheBuilder.build(new RecordWriterLoader());

    cacheCleaner = new CacheCleaner(this.recordWriters, "JdbcTeeProcessor", 10 * 60 * 1000);
  }

  class RecordWriterLoader extends CacheLoader<String, JdbcRecordWriter> {
    @Override
    public JdbcRecordWriter load(String tableName) throws Exception {
      return JdbcRecordReaderWriterFactory.createJdbcRecordWriter(
          hikariConfigBean.getConnectionString(),
          dataSource,
          schema,
          tableName,
          customMappings,
          generatedColumnMappings,
          rollbackOnError,
          useMultiRowOp,
          maxPrepStmtParameters,
          defaultOperation.getCode(),
          unsupportedAction,
          JdbcRecordReaderWriterFactory.createRecordReader(changeLogFormat),
          caseSensitive,
          Collections.emptyList(),
          getContext()
      );
    }
  }

  private final LoadingCache<String, JdbcRecordWriter> recordWriters;

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    Processor.Context context = getContext();

    issues = hikariConfigBean.validateConfigs(context, issues);

    if (hikariConfigBean.getConnectionString().toLowerCase().startsWith("jdbc:sqlserver") && useMultiRowOp) {
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), MULTI_ROW_OP, JdbcErrors.JDBC_57));
    }

    if (dynamicTableName) {
      tableNameVars = getContext().createELVars();
      tableNameEval = context.createELEval(JdbcUtil.TABLE_NAME);
      ELUtils.validateExpression(
          tableNameTemplate,
          getContext(),
          Groups.JDBC.getLabel(),
          JdbcUtil.TABLE_NAME,
          JdbcErrors.JDBC_26,
          issues
      );
    }

    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = jdbcUtil.createDataSourceForWrite(
            hikariConfigBean, schema,
            tableNameTemplate,
            caseSensitive,
            issues,
            customMappings,
            getContext(),
            false
        );
      } catch (StageException e) {
        LOG.error("Could not connect to data source", e);
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, e.getErrorCode(), e.getParams()));
      } catch (RuntimeException | SQLException e) {
        LOG.error("Could not connect to data source", e);
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  @Override
  public void destroy() {
    jdbcUtil.closeQuietly(dataSource);
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
      return;
    }

    boolean perRecord = false;
    // MS SQL Server does not support returning generateKey after executeBatch
    // Instead of executeBatch, do executeUpdate per record
    if (hikariConfigBean.getConnectionString().toLowerCase().startsWith("jdbc:sqlserver")) {
      perRecord = true;
    }

    if (dynamicTableName) {
      jdbcUtil.write(
          batch,
          tableNameEval,
          tableNameVars,
          tableNameTemplate,
          recordWriters,
          errorRecordHandler,
          perRecord
      );
    } else {
      jdbcUtil.write(batch.getRecords(), tableNameTemplate, recordWriters, errorRecordHandler, perRecord);
    }

    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      batchMaker.addRecord(it.next());
    }
  }
}
