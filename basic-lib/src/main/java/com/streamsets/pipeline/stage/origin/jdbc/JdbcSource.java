/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class JdbcSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

  private static final String CONNECTION_STRING = "connectionString";
  private static final String QUERY = "query";
  private static final String OFFSET_COLUMN = "offsetColumn";
  private static final String DRIVER_CLASSNAME = "driverClassName";
  private static final String QUERY_INTERVAL_EL = "queryInterval";

  private final boolean isIncrementalMode;
  private final String connectionString;
  private final String query;
  private final String initialOffset;
  private final String offsetColumn;
  private final String username;
  private final String password;
  private final Properties driverProperties = new Properties();
  private final String driverClassName;
  private final String connectionTestQuery;
  
  private long queryIntervalMillis = Long.MIN_VALUE;

  private HikariDataSource dataSource = null;

  private Connection connection = null;
  private Statement statement = null;
  private ResultSet resultSet = null;
  private long lastQueryCompletedTime = 0L;

  public JdbcSource(
      boolean isIncrementalMode,
      String connectionString,
      String query,
      String initialOffset,
      String offsetColumn,
      long queryInterval,
      String username,
      String password,
      Map<String, String> driverPropertyMap,
      String driverClassName,
      String connectionTestQuery
  ) {
    this.isIncrementalMode = isIncrementalMode;
    this.connectionString = connectionString;
    this.query = query;
    this.initialOffset = initialOffset;
    this.offsetColumn = offsetColumn;
    this.queryIntervalMillis = 1000 * queryInterval;
    this.username = username;
    this.password = password;
    driverProperties.putAll(driverPropertyMap);
    this.driverClassName = driverClassName;
    this.connectionTestQuery = connectionTestQuery;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    Source.Context context = getContext();

    if (queryIntervalMillis < 0) {
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), QUERY_INTERVAL_EL, Errors.JDBC_07));
    }

    if (!driverClassName.isEmpty()) {
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        issues.add(context.createConfigIssue(Groups.LEGACY.name(), DRIVER_CLASSNAME, Errors.JDBC_01, e.toString()));
      }
    }

    final String formattedOffsetColumn = Pattern.quote(offsetColumn.toUpperCase());
    Pattern offsetColumnInWhereAndOrderByClause = Pattern.compile(
        String.format("(?s).*\\bWHERE\\b.*(\\b%s\\b).*\\bORDER BY\\b\\s+\\b%s\\b.*",
            formattedOffsetColumn,
            formattedOffsetColumn
        )
    );

    if (!offsetColumnInWhereAndOrderByClause.matcher(query.toUpperCase()).matches()) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, Errors.JDBC_05, offsetColumn));
    }

    try {
      createDataSource();
      try (Connection connection = dataSource.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.setFetchSize(1);
          statement.setMaxRows(1);
          final String preparedQuery = prepareQuery(query, initialOffset);
          try (ResultSet resultSet = statement.executeQuery(preparedQuery)) {
            try {
              resultSet.findColumn(offsetColumn);
            } catch (SQLException e) {
              logSQLException(e);
              issues.add(
                  context.createConfigIssue(Groups.JDBC.name(), OFFSET_COLUMN, Errors.JDBC_02, offsetColumn, query)
              );
            }
          } catch (SQLException e) {
            logSQLException(e);
            issues.add(
                context.createConfigIssue(Groups.JDBC.name(), QUERY, Errors.JDBC_04, preparedQuery, e.toString())
            );
          }
        }
      } catch (SQLException e) {
        logSQLException(e);
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, Errors.JDBC_00, e.toString()));
      }
    }
    catch (StageException e) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, Errors.JDBC_00, e.toString()));
    }
    return issues;
  }

  @Override
  public void destroy() {
    closeQuietly(connection);
    closeQuietly(dataSource);
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset = lastSourceOffset;

    long now = System.currentTimeMillis();
    long delay = Math.max(0, (lastQueryCompletedTime + queryIntervalMillis) - now);

    LOG.debug("Sleeping for {}ms", delay);
    if (ThreadUtil.sleep(delay)) {
      try {
        if (null == resultSet || resultSet.isClosed()) {
          connection = dataSource.getConnection();
          statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

          int fetchSize = maxBatchSize;
          // MySQL does not support cursors or fetch size except 0 and "streaming" (1 at a time).
          if (connectionString.toLowerCase().contains("mysql")) {
            // Enable MySQL streaming mode.
            fetchSize = Integer.MIN_VALUE;
          }
          statement.setFetchSize(fetchSize);
          LOG.debug("Using query fetch size: {}", fetchSize);

          if (getContext().isPreview()) {
            statement.setMaxRows(maxBatchSize);
          }
          resultSet = statement.executeQuery(prepareQuery(query, lastSourceOffset));
        }
        // Read Data and track last offset
        int rowCount = 0;
        while (rowCount < maxBatchSize && resultSet.next()) {
          final Record record = processRow(resultSet);
          if (null != record) {
            batchMaker.addRecord(record);
          }

          // Get the offset column value for this record
          if (isIncrementalMode) {
            nextSourceOffset = resultSet.getString(offsetColumn);
          } else {
            nextSourceOffset = initialOffset;
          }
          ++rowCount;
        }
        // isAfterLast is not required to be implemented if using FORWARD_ONLY cursor.
        if (resultSet.isAfterLast() || rowCount == 0) {
          // We didn't have any data left in the cursor.
          closeQuietly(connection);
          lastQueryCompletedTime = System.currentTimeMillis();
          LOG.debug("Query completed at: {}", lastQueryCompletedTime);
        }
      } catch (SQLException e) {
        logSQLException(e);
        closeQuietly(connection);
        lastQueryCompletedTime = System.currentTimeMillis();
        LOG.debug("Query failed at: {}", lastQueryCompletedTime);
        handleError(Errors.JDBC_04, prepareQuery(query, lastSourceOffset), e.toString());
      }
    }
    return nextSourceOffset;
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

  private void createDataSource() throws StageException {
    if (null != dataSource) {
      return;
    }

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionString);
    config.setUsername(username);
    config.setPassword(password);
    // These do not need to be user-configurable
    config.setReadOnly(true);
    config.setMaximumPoolSize(2);
    if (!connectionTestQuery.isEmpty()) {
      config.setConnectionTestQuery(connectionTestQuery);
    }
    // User configurable JDBC driver properties
    config.setDataSourceProperties(driverProperties);

    try {
      dataSource = new HikariDataSource(config);
    } catch (RuntimeException e) {
      LOG.error(Errors.JDBC_06.getMessage(), e);
      throw new StageException(Errors.JDBC_06, e.getCause().toString());
    }
  }

  private String prepareQuery(String query, String lastSourceOffset) {
    final String offset = null == lastSourceOffset ? initialOffset : lastSourceOffset;
    return query.replaceAll("\\$\\{offset\\}", offset);
  }

  private Record processRow(ResultSet resultSet) throws SQLException, StageException {
    Source.Context context = getContext();
    ResultSetMetaData md = resultSet.getMetaData();
    int numColumns = md.getColumnCount();
    Map<String, Field> fields = new HashMap<>(numColumns);

    // Process row
    for (int i = 1; i <= numColumns; i++) {
      Object value = resultSet.getObject(i);
      try {
        fields.put(md.getColumnName(i), JsonUtil.jsonToField(value));
      } catch (IOException e) {
        handleError(Errors.JDBC_03, md.getColumnName(i), value);
      }
    }

    if (fields.size() != numColumns) {
      return null; // Don't output this record.
    }

    final String recordContext = query + "::" + resultSet.getString(offsetColumn);
    Record record = context.createRecord(recordContext);
    record.set(Field.create(fields));
    return record;
  }

  private void handleError(ErrorCode errorCode, Object... params) throws StageException {
    Source.Context context = getContext();
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.reportError(errorCode, params);
        break;
      case STOP_PIPELINE:
        throw new StageException(errorCode, params);
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
            context.getOnErrorRecord()));
    }
  }
  
  static void logSQLException(SQLException e) {
    LOG.error("SQLException: {}", e.toString());
    SQLException next = e.getNextException();
    if (null != next) {
      logSQLException(e.getNextException());
    }
  }
}
