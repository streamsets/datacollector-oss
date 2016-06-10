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
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcLookupProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLookupProcessor.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private ELEval queryEval;

  private final String query;
  private final List<JdbcFieldColumnMapping> columnMappings;
  private final int maxClobSize;
  private final int maxBlobSize;
  private final HikariPoolConfigBean hikariConfigBean;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private Map<String, String> columnsToFields = new HashMap<>();
  private final Properties driverProperties = new Properties();

  public JdbcLookupProcessor(
      String query,
      List<JdbcFieldColumnMapping> columnMappings,
      int maxClobSize,
      int maxBlobSize,
      HikariPoolConfigBean hikariConfigBean
  ) {
    this.query = query;
    this.columnMappings = columnMappings;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.hikariConfigBean = hikariConfigBean;
    driverProperties.putAll(hikariConfigBean.driverProperties);
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    Processor.Context context = getContext();

    queryEval = getContext().createELEval("query");

    issues = hikariConfigBean.validateConfigs(context, issues);

    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
      } catch (StageException e) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }

    for (JdbcFieldColumnMapping mapping : columnMappings) {
      LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
      columnsToFields.put(mapping.columnName, mapping.field);
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    closeQuietly(connection);
    closeQuietly(dataSource);
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      connection = dataSource.getConnection();

      lookupValuesForRecord(record);
      batchMaker.addRecord(record);
    } catch (OnRecordErrorException error) {
      errorRecordHandler.onError(error);
    } catch (SQLException e) {
      String formattedError = JdbcUtil.formatSqlException(e);
      LOG.error(formattedError, e);
      closeQuietly(connection);
      LOG.error("Query failed at: {}", System.currentTimeMillis());
    } finally {
      closeQuietly(connection);
      connection = null;
    }
  }

  private void lookupValuesForRecord(Record record) throws StageException {
    ELVars elVars = getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);

    String preparedQuery;
    try {
      preparedQuery = queryEval.eval(elVars, query, String.class);
    } catch (ELEvalException e) {
      LOG.error(JdbcErrors.JDBC_01.getMessage(), query, e);
      throw new OnRecordErrorException(record, JdbcErrors.JDBC_01, query);
    }

    try (Statement stmt = connection.createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(preparedQuery)) {
        if (resultSet.next()) {
          ResultSetMetaData md = resultSet.getMetaData();

          LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(
              resultSet,
              maxClobSize,
              maxBlobSize,
              errorRecordHandler
          );

          int numColumns = md.getColumnCount();
          if (fields.size() != numColumns) {
            errorRecordHandler.onError(JdbcErrors.JDBC_35, fields.size(), numColumns);
          }

          for (Map.Entry<String, Field> entry: fields.entrySet()) {
            String columnName = entry.getKey();
            String fieldPath = columnsToFields.get(columnName);
            if (fieldPath == null) {
              LOG.error(JdbcErrors.JDBC_25.getMessage(), columnName);
              errorRecordHandler.onError(JdbcErrors.JDBC_25, columnName);
            }
            record.set(fieldPath, entry.getValue());
          }
        } else {
          // No results
          LOG.error(JdbcErrors.JDBC_04.getMessage(), preparedQuery);
          throw new OnRecordErrorException(record, JdbcErrors.JDBC_04, preparedQuery);
        }
      }
    } catch (SQLException e) {
      // Exception executing query
      LOG.error(JdbcErrors.JDBC_02.getMessage(), preparedQuery, e);
      throw new OnRecordErrorException(record, JdbcErrors.JDBC_02, preparedQuery, e.getMessage());
    }
  }

  private void closeQuietly(AutoCloseable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception ex) {
        LOG.debug("Error while closing: {}", ex.toString(), ex);
      }
    }
  }
}