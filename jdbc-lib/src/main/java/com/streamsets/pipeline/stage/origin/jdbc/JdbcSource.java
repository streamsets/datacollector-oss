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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.MILLISECONDS;

public class JdbcSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String QUERY = "query";
  private static final String OFFSET_COLUMN = "offsetColumn";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String QUERY_INTERVAL_EL = "queryInterval";
  private static final String TXN_ID_COLUMN_NAME = "txnIdColumnName";
  private static final String TXN_MAX_SIZE = "txnMaxSize";
  private static final String MAX_BATCH_SIZE = "maxBatchSize";
  private static final String MAX_CLOB_SIZE = "maxClobSize";
  private static final String JDBC_NS_HEADER_PREFIX = "jdbcNsHeaderPrefix";

  private final boolean isIncrementalMode;
  private final String query;
  private final String initialOffset;
  private final String offsetColumn;
  private final Properties driverProperties = new Properties();
  private final String txnColumnName;
  private final int txnMaxSize;
  private final JdbcRecordType recordType;
  private final int maxBatchSize;
  private final int maxClobSize;
  private final int maxBlobSize;
  private final HikariPoolConfigBean hikariConfigBean;
  private final boolean createJDBCNsHeaders;
  private final String jdbcNsHeaderPrefix;


  private ErrorRecordHandler errorRecordHandler;

  private long queryIntervalMillis = Long.MIN_VALUE;

  private HikariDataSource dataSource = null;

  private Connection connection = null;
  private ResultSet resultSet = null;
  private long lastQueryCompletedTime = 0L;

  public JdbcSource(
      boolean isIncrementalMode,
      String query,
      String initialOffset,
      String offsetColumn,
      long queryInterval,
      String txnColumnName,
      int txnMaxSize,
      JdbcRecordType jdbcRecordType,
      int maxBatchSize,
      int maxClobSize,
      int maxBlobSize,
      boolean createJDBCNsHeaders,
      String jdbcNsHeaderPrefix,
      HikariPoolConfigBean hikariConfigBean
  ) {
    this.isIncrementalMode = isIncrementalMode;
    this.query = query;
    this.initialOffset = initialOffset;
    this.offsetColumn = offsetColumn;
    this.queryIntervalMillis = 1000 * queryInterval;
    driverProperties.putAll(hikariConfigBean.driverProperties);
    this.txnColumnName = txnColumnName;
    this.txnMaxSize = txnMaxSize;
    this.recordType = jdbcRecordType;
    this.maxBatchSize = maxBatchSize;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.hikariConfigBean = hikariConfigBean;
    this.createJDBCNsHeaders = createJDBCNsHeaders;
    this.jdbcNsHeaderPrefix = jdbcNsHeaderPrefix;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    Source.Context context = getContext();

    errorRecordHandler = new DefaultErrorRecordHandler(context);
    issues = hikariConfigBean.validateConfigs(context, issues);

    if (queryIntervalMillis < 0) {
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), QUERY_INTERVAL_EL, Errors.JDBC_07));
    }

    if (!hikariConfigBean.driverClassName.isEmpty()) {
      try {
        Class.forName(hikariConfigBean.driverClassName);
      } catch (ClassNotFoundException e) {
        issues.add(context.createConfigIssue(Groups.LEGACY.name(), DRIVER_CLASSNAME, Errors.JDBC_01, e.toString()));
      }
    }

    final String formattedOffsetColumn = Pattern.quote(offsetColumn.toUpperCase());
    Pattern offsetColumnInWhereAndOrderByClause = Pattern.compile(
        String.format("(?s).*\\bWHERE\\b.*(\\b%s\\b).*\\bORDER BY\\b.*\\b%s\\b.*",
            formattedOffsetColumn,
            formattedOffsetColumn
        )
    );

    if (!offsetColumnInWhereAndOrderByClause.matcher(query.toUpperCase()).matches()) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, Errors.JDBC_05, offsetColumn));
    }

    if (txnMaxSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), TXN_MAX_SIZE, Errors.JDBC_10, txnMaxSize, 0));
    }
    if (maxBatchSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), MAX_BATCH_SIZE, Errors.JDBC_10, maxBatchSize, 0));
    }
    if (maxClobSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), MAX_CLOB_SIZE, Errors.JDBC_10, maxClobSize, 0));
    }

    if (issues.isEmpty()) {
      try {
        createDataSource();
        try (Connection connection = dataSource.getConnection()) {
          DatabaseMetaData dbMetadata = connection.getMetaData();
          // If CDC is enabled, scrollable cursors must be supported by JDBC driver.
          if (!txnColumnName.isEmpty() && !dbMetadata.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
            issues.add(context.createConfigIssue(Groups.CDC.name(), TXN_ID_COLUMN_NAME, Errors.JDBC_12));
          }
          try (Statement statement = connection.createStatement()) {
            statement.setFetchSize(1);
            statement.setMaxRows(1);
            final String preparedQuery = prepareQuery(query, initialOffset);
            try (ResultSet resultSet = statement.executeQuery(preparedQuery)) {
              try {
                Set<String> columnLabels = new HashSet<>();
                ResultSetMetaData metadata = resultSet.getMetaData();
                int columnIdx = metadata.getColumnCount() + 1;
                while (--columnIdx > 0) {
                  String columnLabel = metadata.getColumnLabel(columnIdx);
                  if (columnLabels.contains(columnLabel)) {
                    issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, Errors.JDBC_08, columnLabel));
                  } else {
                    columnLabels.add(columnLabel);
                  }
                }
                if (offsetColumn.contains(".")) {
                  issues.add(context.createConfigIssue(Groups.JDBC.name(), OFFSET_COLUMN, Errors.JDBC_09, offsetColumn));
                } else {
                  resultSet.findColumn(offsetColumn);
                }
              } catch (SQLException e) {
                // Log a warning instead of an error because some implementations such as Oracle have implicit
                // "columns" such as ROWNUM that won't appear as part of the resultset.
                LOG.warn(Errors.JDBC_02.getMessage(), offsetColumn, query);
                LOG.warn(JdbcUtil.formatSqlException(e));
              }
            } catch (SQLException e) {
              String formattedError = JdbcUtil.formatSqlException(e);
              LOG.error(formattedError);
              LOG.debug(formattedError, e);
              issues.add(
                  context.createConfigIssue(Groups.JDBC.name(), QUERY, Errors.JDBC_04, preparedQuery, formattedError)
              );
            }
          }
        } catch (SQLException e) {
          String formattedError = JdbcUtil.formatSqlException(e);
          LOG.error(formattedError);
          LOG.debug(formattedError, e);
          issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, Errors.JDBC_00, formattedError));
        }
      } catch (StageException e) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, Errors.JDBC_00, e.toString()));
      }
    }
    if (createJDBCNsHeaders && !jdbcNsHeaderPrefix.endsWith(".")) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), JDBC_NS_HEADER_PREFIX, Errors.JDBC_15));
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
    int batchSize = Math.min(this.maxBatchSize, maxBatchSize);
    String nextSourceOffset = lastSourceOffset == null ? initialOffset : lastSourceOffset;

    long now = System.currentTimeMillis();
    long delay = Math.max(0, (lastQueryCompletedTime + queryIntervalMillis) - now);

    if (delay > 0) {
      // Sleep in one second increments so we don't tie up the app.
      LOG.debug("{}ms remaining until next fetch.", delay);
      ThreadUtil.sleep(Math.min(delay, 1000));
    } else {
      Statement statement = null;
      try {
        if (null == resultSet || resultSet.isClosed()) {
          connection = dataSource.getConnection();

          if (!txnColumnName.isEmpty()) {
            // CDC requires scrollable cursors.
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          } else {
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
          }

          int fetchSize = batchSize;
          // MySQL does not support cursors or fetch size except 0 and "streaming" (1 at a time).
          if (hikariConfigBean.connectionString.toLowerCase().contains("mysql")) {
            // Enable MySQL streaming mode.
            fetchSize = Integer.MIN_VALUE;
          }
          LOG.debug("Using query fetch size: {}", fetchSize);
          statement.setFetchSize(fetchSize);

          if (getContext().isPreview()) {
            statement.setMaxRows(batchSize);
          }
          String preparedQuery = prepareQuery(query, lastSourceOffset);
          LOG.debug("Executing query: " + preparedQuery);
          resultSet = statement.executeQuery(preparedQuery);
        }
        // Read Data and track last offset
        int rowCount = 0;
        String lastTransactionId = "";
        while (continueReading(rowCount, batchSize) && resultSet.next()) {
          final Record record = processRow(resultSet);

          if (null != record) {
            if (!txnColumnName.isEmpty()) {
              String newTransactionId = resultSet.getString(txnColumnName);
              if (lastTransactionId.isEmpty()) {
                lastTransactionId = newTransactionId;
                batchMaker.addRecord(record);
              } else if (lastTransactionId.equals(newTransactionId)) {
                batchMaker.addRecord(record);
              } else {
                // The Transaction ID Column Name config should not be used with MySQL as it
                // does not provide a change log table and the JDBC driver may not support scrollable cursors.
                resultSet.relative(-1);
                break; // Complete this batch without including the new record.
              }
            } else {
              batchMaker.addRecord(record);
            }
          }

          // Get the offset column value for this record
          if (isIncrementalMode) {
            nextSourceOffset = resultSet.getString(offsetColumn);
          } else {
            nextSourceOffset = initialOffset;
          }
          ++rowCount;
        }
        LOG.debug("Processed rows: " + rowCount);

        // isAfterLast is not required to be implemented if using FORWARD_ONLY cursor.
        if (resultSet.isAfterLast() || rowCount == 0) {
          // We didn't have any data left in the cursor.
          closeQuietly(connection);
          lastQueryCompletedTime = System.currentTimeMillis();
          LOG.debug("Query completed at: {}", lastQueryCompletedTime);
        }
      } catch (SQLException e) {
        String formattedError = JdbcUtil.formatSqlException(e);
        LOG.error(formattedError);
        LOG.debug(formattedError, e);
        closeQuietly(connection);
        lastQueryCompletedTime = System.currentTimeMillis();
        LOG.debug("Query failed at: {}", lastQueryCompletedTime);
        errorRecordHandler.onError(Errors.JDBC_04, prepareQuery(query, lastSourceOffset), formattedError);
      }
    }
    return nextSourceOffset;
  }

  private boolean continueReading(int rowCount, int batchSize) {
    if (txnColumnName.isEmpty()) {
      return rowCount < batchSize;
    } else {
      return rowCount < txnMaxSize;
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

  private void createDataSource() throws StageException {
    if (null != dataSource) {
      return;
    }

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(hikariConfigBean.connectionString);
    config.setUsername(hikariConfigBean.username);
    config.setPassword(hikariConfigBean.password);
    config.setReadOnly(hikariConfigBean.readOnly);
    config.setMaximumPoolSize(hikariConfigBean.maximumPoolSize);
    config.setMinimumIdle(hikariConfigBean.minIdle);
    config.setConnectionTimeout(hikariConfigBean.connectionTimeout * MILLISECONDS);
    config.setIdleTimeout(hikariConfigBean.idleTimeout * MILLISECONDS);
    config.setMaxLifetime(hikariConfigBean.maxLifetime * MILLISECONDS);

    if (!hikariConfigBean.connectionTestQuery.isEmpty()) {
      config.setConnectionTestQuery(hikariConfigBean.connectionTestQuery);
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

  private String getClobString(Clob data) throws IOException, SQLException {
    if (data == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    int bufLen = 1024;
    char[] cbuf = new char[bufLen];

    // Read up to max clob length
    long maxRemaining = maxClobSize;
    int count;
    Reader r = data.getCharacterStream();
    while ((count = r.read(cbuf)) > -1 && maxRemaining > 0) {
      // If c is more then the remaining chars we want to read, read only as many are available
      if (count > maxRemaining) {
        count = (int) maxRemaining;
      }
      sb.append(cbuf, 0, count);
      // decrement available according to the number of chars we've read
      maxRemaining -= count;
    }
    return sb.toString();
  }

  private byte[] getBlobBytes(Blob data) throws IOException, SQLException {
    if (data == null) {
      return new byte[0];
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    int bufLen = 1024;
    byte[] buf = new byte[bufLen];

    // Read up to max blob length
    long maxRemaining = maxBlobSize;
    int count;
    InputStream is = data.getBinaryStream();
    while ((count = is.read(buf)) > -1 && maxRemaining > 0) {
      // If count is more then the remaining bytes we want to read, read only as many are available
      if (count > maxRemaining) {
        count = (int) maxRemaining;
      }
      os.write(buf, 0, count);
      // decrement available according to the number of bytes we've read
      maxRemaining -= count;
    }
    return os.toByteArray();
  }

  private Record processRow(ResultSet resultSet) throws SQLException, StageException {
    Source.Context context = getContext();
    ResultSetMetaData md = resultSet.getMetaData();
    int numColumns = md.getColumnCount();

    // Generate fields
    LinkedHashMap<String, Field> fields = resultSetToFields(resultSet);

    if (fields.size() != numColumns) {
      errorRecordHandler.onError(Errors.JDBC_14, fields.size(), numColumns);
      return null; // Don't output this record.
    }

    final String recordContext = query + "::" + resultSet.getString(offsetColumn);
    Record record = context.createRecord(recordContext);
    if (recordType == JdbcRecordType.LIST_MAP) {
      record.set(Field.createListMap(fields));
    } else if (recordType == JdbcRecordType.MAP) {
      record.set(Field.create(fields));
    } else {
      // type is LIST
      List<Field> row = new ArrayList<>();
      for (Map.Entry<String, Field> fieldInfo : fields.entrySet()) {
        Map<String, Field> cell = new HashMap<>();
        cell.put("header", Field.create(fieldInfo.getKey()));
        cell.put("value", fieldInfo.getValue());
        row.add(Field.create(cell));
      }
      record.set(Field.create(row));
    }
    if (createJDBCNsHeaders) {
      JdbcUtil.setColumnSpecificHeaders(record, md, jdbcNsHeaderPrefix);
    }
    return record;
  }

  private LinkedHashMap<String, Field> resultSetToFields(ResultSet rs) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>(md.getColumnCount());

    for (int i = 1; i <= md.getColumnCount(); i++) {
      Object value = rs.getObject(i);
      try {
        Field field;
        // All types as of JDBC 2.0 are here:
        // https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
        // Good source of recommended mappings is here:
        // http://www.cs.mun.ca/java-api-1.5/guide/jdbc/getstart/mapping.html
        switch (md.getColumnType(i)) {
          case Types.BIGINT:
            field = Field.create(Field.Type.LONG, rs.getObject(i));
            break;
          case Types.BINARY:
          case Types.LONGVARBINARY:
          case Types.VARBINARY:
            field = Field.create(Field.Type.BYTE_ARRAY, rs.getObject(i));
            break;
          case Types.BIT:
          case Types.BOOLEAN:
            field = Field.create(Field.Type.BOOLEAN, rs.getObject(i));
            break;
          case Types.CHAR:
          case Types.LONGNVARCHAR:
          case Types.LONGVARCHAR:
          case Types.NCHAR:
          case Types.NVARCHAR:
          case Types.VARCHAR:
            field = Field.create(Field.Type.STRING, rs.getObject(i));
            break;
          case Types.CLOB:
          case Types.NCLOB:
            field = Field.create(Field.Type.STRING, getClobString(rs.getClob(i)));
            break;
          case Types.BLOB:
            field = Field.create(Field.Type.BYTE_ARRAY, getBlobBytes(rs.getBlob(i)));
            break;
          case Types.DATE:
            field = Field.create(Field.Type.DATE, rs.getDate(i));
            break;
          case Types.DECIMAL:
          case Types.NUMERIC:
            field = Field.create(Field.Type.DECIMAL, rs.getBigDecimal(i));
            break;
          case Types.DOUBLE:
            field = Field.create(Field.Type.DOUBLE, rs.getObject(i));
            break;
          case Types.FLOAT:
          case Types.REAL:
            field = Field.create(Field.Type.FLOAT, rs.getObject(i));
            break;
          case Types.INTEGER:
            field = Field.create(Field.Type.INTEGER, rs.getObject(i));
            break;
          case Types.ROWID:
            field = Field.create(Field.Type.STRING, rs.getRowId(i).toString());
            break;
          case Types.SMALLINT:
          case Types.TINYINT:
            field = Field.create(Field.Type.SHORT, rs.getObject(i));
            break;
          case Types.TIME:
            field = Field.create(Field.Type.TIME, rs.getObject(i));
            break;
          case Types.TIMESTAMP:
            field = Field.create(Field.Type.DATETIME, rs.getObject(i));
            break;
          case Types.ARRAY:
          case Types.DATALINK:
          case Types.DISTINCT:
          case Types.JAVA_OBJECT:
          case Types.NULL:
          case Types.OTHER:
          case Types.REF:
            //case Types.REF_CURSOR: // JDK8 only
          case Types.SQLXML:
          case Types.STRUCT:
            //case Types.TIME_WITH_TIMEZONE: // JDK8 only
            //case Types.TIMESTAMP_WITH_TIMEZONE: // JDK8 only
          default:
            throw new StageException(Errors.JDBC_16, md.getColumnType(i), md.getColumnLabel(i));
        }

        fields.put(md.getColumnLabel(i), field);
      } catch (SQLException e) {
        errorRecordHandler.onError(Errors.JDBC_13, e.getMessage(), e);
      } catch (IOException e) {
        errorRecordHandler.onError(Errors.JDBC_03, md.getColumnName(i), value, e);
      }
    }

    return fields;
  }
}
