/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.ChangeLogFormat;
import com.streamsets.pipeline.lib.jdbc.JdbcGenericRecordWriter;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * JDBC Destination for StreamSets Data Collector
 */
public class JdbcTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTarget.class);

  private static final String CUSTOM_MAPPINGS = "columnNames";
  private static final String TABLE_NAME = "tableName";
  private static final String CONNECTION_STRING = "connectionString";
  private static final String DRIVER_CLASSNAME = "driverClassName";

  private final String connectionString;
  private final String username;
  private final String password;
  private final boolean rollbackOnError;

  private final String tableName;
  private final List<JdbcFieldMappingConfig> customMappings;

  private final Properties driverProperties = new Properties();
  private final String driverClassName;
  private final String connectionTestQuery;
  private final ChangeLogFormat changeLogFormat;

  private HikariDataSource dataSource = null;

  private Connection connection = null;
  private JdbcRecordWriter recordWriter;

  public JdbcTarget(
      final String connectionString,
      final String username,
      final String password,
      final String tableName,
      final List<JdbcFieldMappingConfig> customMappings,
      final boolean rollbackOnError,
      final Map<String, String> driverProperties,
      final ChangeLogFormat changeLogFormat,
      final String driverClassName,
      final String connectionTestQuery
  ) {
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.tableName = tableName;
    this.customMappings = customMappings;
    this.rollbackOnError = rollbackOnError;
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

    if (createDataSource(issues)) {
      try {
        createRecordWriter();
      } catch (StageException e) {
        issues.add(context.createConfigIssue(
                Groups.JDBC.name(),
                changeLogFormat.getLabel(),
                Errors.JDBCDEST_13,
                e.toString()
            )
        );
      }
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
    } catch (Exception ignored) {}
  }

  private void createRecordWriter() throws StageException {
    switch (changeLogFormat) {
      case NONE:
        recordWriter = new JdbcGenericRecordWriter(connectionString, dataSource, tableName, rollbackOnError, customMappings);
        break;
      case MSSQL:
        recordWriter = new MicrosoftJdbcRecordWriter(connectionString, dataSource, tableName);
        break;
      default:
        throw new IllegalStateException("Unrecognized format specified: " + changeLogFormat);
    }
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
    // These do not need to be user-configurable
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
      ResultSet res = JdbcUtil.getTableMetadata(connection, tableName);
      if (!res.next()) {
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), TABLE_NAME, Errors.JDBCDEST_16, tableName));
      } else {
        ResultSet columns = JdbcUtil.getColumnMetadata(connection, tableName);
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
      List<OnRecordErrorException> errors = recordWriter.writeBatch(batch);
      for (OnRecordErrorException error : errors) {
        handleErrorRecord(error);
      }
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
            Utils.format("It should never happen. OnError '{}'", getContext().getOnErrorRecord(), error)
        );
    }
  }
}