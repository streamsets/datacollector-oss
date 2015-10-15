/**
 * Copyright 2015 StreamSets Inc.
 * <p/>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.jdbc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.ChangeLogFormat;
import com.streamsets.pipeline.lib.jdbc.JdbcGenericRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcMultiRowRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.MicrosoftJdbcRecordWriter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * JDBC Destination for StreamSets Data Collector
 */
public class JdbcTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTarget.class);

  private static final String CUSTOM_MAPPINGS = "columnNames";
  private static final String TABLE_NAME = "tableNameTemplate";
  private static final String CONNECTION_STRING = "connectionString";
  private static final String DRIVER_CLASSNAME = "driverClassName";
  private static final String EL_PREFIX = "${";

  private final String connectionString;
  private final String username;
  private final String password;
  private final boolean rollbackOnError;
  private final boolean useMultiRowInsert;

  private final String tableNameTemplate;
  private final List<JdbcFieldMappingConfig> customMappings;

  private final Properties driverProperties = new Properties();
  private final String driverClassName;
  private final String connectionTestQuery;
  private final ChangeLogFormat changeLogFormat;

  private HikariDataSource dataSource = null;
  private ELEval tableNameEval = null;

  private Connection connection = null;

  class RecordWriterLoader extends CacheLoader<String, JdbcRecordWriter> {
    @Override
    public JdbcRecordWriter load(String tableName) throws Exception {
      return createRecordWriter(tableName);
    }
  }

  private final LoadingCache<String, JdbcRecordWriter> recordWriters = CacheBuilder.newBuilder()
      .maximumSize(500)
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new RecordWriterLoader());

  public JdbcTarget(
      final String connectionString,
      final String username,
      final String password,
      final String tableNameTemplate,
      final List<JdbcFieldMappingConfig> customMappings,
      final boolean rollbackOnError,
      final boolean useMultiRowInsert,
      final Map<String, String> driverProperties,
      final ChangeLogFormat changeLogFormat,
      final String driverClassName,
      final String connectionTestQuery
  ) {
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.tableNameTemplate = tableNameTemplate;
    this.customMappings = customMappings;
    this.rollbackOnError = rollbackOnError;
    this.useMultiRowInsert = useMultiRowInsert;
    if (driverProperties != null) {
      this.driverProperties.putAll(driverProperties);
    }
    this.changeLogFormat = changeLogFormat;
    this.driverClassName = driverClassName;
    this.connectionTestQuery = connectionTestQuery;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    Target.Context context = getContext();

    tableNameEval = context.createELEval("tableNameTemplate");
    validateEL(tableNameEval, tableNameTemplate, "tableNameTemplate", Errors.JDBCDEST_20, Errors.JDBCDEST_21, issues);

    if (issues.isEmpty()) {
      createDataSource(issues);
    }

    return issues;
  }

  @Override
  public void destroy() {
    closeQuietly(connection);

    if (null != dataSource) {
      dataSource.close();
    }
    super.destroy();
  }

  private void closeQuietly(AutoCloseable c) {
    try {
      if (null != c) {
        c.close();
      }
    } catch (Exception ignored) {
    }
  }

  private JdbcRecordWriter createRecordWriter(String tableName) throws StageException {
    JdbcRecordWriter recordWriter;

    switch (changeLogFormat) {
      case NONE:
        if (!useMultiRowInsert) {
          recordWriter = new JdbcGenericRecordWriter(
              connectionString,
              dataSource,
              tableName,
              rollbackOnError,
              customMappings
          );
        } else {
          recordWriter = new JdbcMultiRowRecordWriter(
              connectionString,
              dataSource,
              tableName,
              rollbackOnError,
              customMappings
          );
        }
        break;
      case MSSQL:
        recordWriter = new MicrosoftJdbcRecordWriter(connectionString, dataSource, tableName);
        break;
      default:
        throw new IllegalStateException("Unrecognized format specified: " + changeLogFormat);
    }
    return recordWriter;
  }

  private boolean createDataSource(List<ConfigIssue> issues) {
    if (null != dataSource) {
      return false;
    }

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionString);
    config.setUsername(username);
    config.setPassword(password);
    config.setAutoCommit(false);
    if (driverClassName != null && !driverClassName.isEmpty()) {
      config.setDriverClassName(driverClassName);
    }
    // These do not need to be user-configurable. 2 Is required for testing using H2 in-mem.
    config.setMaximumPoolSize(2);
    if (connectionTestQuery != null && !connectionTestQuery.isEmpty()) {
      config.setConnectionTestQuery(connectionTestQuery);
    }
    // User configurable JDBC driver properties
    config.setDataSourceProperties(driverProperties);

    try {
      dataSource = new HikariDataSource(config);

      // Test connectivity
      connection = dataSource.getConnection();

      // Can only validate schema if the user specified a single table.
      if (!tableNameTemplate.contains(EL_PREFIX)) {
        ResultSet res = JdbcUtil.getTableMetadata(connection, tableNameTemplate);
        if (!res.next()) {
          issues.add(getContext().createConfigIssue(Groups.JDBC.name(), TABLE_NAME, Errors.JDBCDEST_16, tableNameTemplate));
        } else {
          ResultSet columns = JdbcUtil.getColumnMetadata(connection, tableNameTemplate);
          Set<String> columnNames = new HashSet<>();
          while (columns.next()) {
            columnNames.add(columns.getString(4));
          }
          for (JdbcFieldMappingConfig customMapping : customMappings) {
            if (!columnNames.contains(customMapping.columnName)) {
              issues.add(getContext().createConfigIssue(
                  Groups.JDBC.name(), CUSTOM_MAPPINGS, Errors.JDBCDEST_04, customMapping.field, customMapping.columnName
                  )
              );
            }
          }
        }
      }
      connection.close();
    } catch (RuntimeException | SQLException e) {
      LOG.debug("Could not connect to data source", e);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, Errors.JDBCDEST_00, e.toString()));
      return false;
    }

    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Batch batch) throws StageException {
    Multimap<String, Record> partitions = partitionBatch(batch);
    Set<String> tableNames = partitions.keySet();
    for (String tableName : tableNames) {
      List<OnRecordErrorException> errors = recordWriters.getUnchecked(tableName).writeBatch(partitions.get(tableName));
      for (OnRecordErrorException error : errors) {
        handleErrorRecord(error);
      }
    }
  }

  private Multimap<String, Record> partitionBatch(Batch batch) {
    Multimap<String, Record> partitions = ArrayListMultimap.create();

    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();

      ELVars vars = getContext().createELVars();
      RecordEL.setRecordInContext(vars, record);

      try {
        String tableName = tableNameEval.eval(vars, tableNameTemplate, String.class);
        partitions.put(tableName, record);
      } catch (ELEvalException e) {
        LOG.error("Failed to evaluate expression '{}' : ", tableNameTemplate, e.toString(), e);
        // send to error
      }
    }

    return partitions;
  }

  private void handleErrorRecord(OnRecordErrorException error) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(error.getRecord(), error);
        break;
      case STOP_PIPELINE:
        throw error;
      default:
        throw new IllegalStateException(
            Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), error)
        );
    }
  }

  private void validateEL(
      ELEval elEval,
      String elStr,
      String config,
      ErrorCode parseError,
      ErrorCode evalError,
      List<ConfigIssue> issues
  ) {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, getContext().createRecord("validateConfigs"));
    boolean parsed = false;
    try {
      getContext().parseEL(elStr);
      parsed = true;
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), config, parseError, ex.toString(), ex));
    }
    if (parsed) {
      try {
        elEval.eval(vars, elStr, String.class);
      } catch (ELEvalException ex) {
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), config, evalError, ex.toString(), ex));
      }
    }
  }
}
