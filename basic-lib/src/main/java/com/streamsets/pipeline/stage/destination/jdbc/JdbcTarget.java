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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC Destination for StreamSets Data Collector
 */
public class JdbcTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTarget.class);

  private static final String CONNECTION_STRING = "connectionString";
  private static final String DRIVER_CLASSNAME = "driverClassName";

  private final String connectionString;
  private final String username;
  private final String password;
  private final boolean rollbackOnError;

  private final String qualifiedTableName;
  private final List<JdbcFieldMappingConfig> columnNames;

  private final Properties driverProperties = new Properties();
  private final String driverClassName;
  private final String connectionTestQuery;

  private LinkedHashMap<String, String> columnMappings;

  private HikariDataSource dataSource = null;

  private Connection connection = null;
  private PreparedStatement statement = null;
  private String queryString;

  public JdbcTarget(
      final String connectionString,
      final String username,
      final String password,
      final String qualifiedTableName,
      final List<JdbcFieldMappingConfig> columnNames,
      final boolean rollbackOnError,
      final Map<String, String> driverProperties,
      final String driverClassName,
      final String connectionTestQuery
  ) {
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.qualifiedTableName = qualifiedTableName;
    this.columnNames = columnNames;
    this.rollbackOnError = rollbackOnError;
    if (driverProperties != null) {
      this.driverProperties.putAll(driverProperties);
    }
    this.driverClassName = driverClassName;
    this.connectionTestQuery = connectionTestQuery;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    setColumnMappings();

    // The INSERT query we're going to perform (parameterized).
    queryString = String.format(
        "INSERT INTO %s (%s) VALUES (%s)",
        qualifiedTableName,
        Joiner.on(", ").join(columnMappings.keySet()),
        Joiner.on(", ").join(Collections.nCopies(columnNames.size(), "?"))
    );
    LOG.debug("Prepared Query: {}", queryString);

    Target.Context context = getContext();

    if (driverClassName != null && !driverClassName.isEmpty()) {
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        issues.add(context.createConfigIssue(Groups.LEGACY.name(),
            DRIVER_CLASSNAME, Errors.JDBCDEST_01, e.toString()));
      }
    }

    if (createDataSource(issues)) {
      validateColumnMapping(issues);
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

  private boolean createDataSource(List<ConfigIssue> issues) {
    if (null != dataSource) {
      return false;
    }

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionString);
    config.setUsername(username);
    config.setPassword(password);
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
      Statement stmt = connection.createStatement();
      stmt.close();
      connection.close();
    } catch (RuntimeException | SQLException e) {
      LOG.debug("Could not connect to data source", e);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, Errors.JDBCDEST_00, e.toString()));
      return false;
    }

    return true;
  }

  private boolean validateColumnMapping(List<ConfigIssue> issues) {
    boolean success = true;
    try {
      String[] tableParts = qualifiedTableName.split("\\.");
      String schema;
      String table;

      // Some databases require that you specify the schema and table separately (H2, which is used for unit-testing,
      // is one of these). Certain databases also have more complex qualified names and, like SQLServer, can have
      // multiple periods separating namespaces. To deal with this, I am just assuming that the last element of the list
      // is the table name, and the second to last is the schema name.
      if (tableParts.length == 1) {
        table = tableParts[0];
        schema = null;
      } else {
        table = tableParts[tableParts.length - 1];
        schema = tableParts[tableParts.length - 2];
      }

      connection = dataSource.getConnection();
      ResultSet rs = connection.getMetaData().getColumns(schema, null, table, null);
      HashSet<String> dbColNames = Sets.newHashSet();
      while (rs.next()) {
        dbColNames.add(rs.getString("COLUMN_NAME").toLowerCase());
      }
      rs.close();

      // Make sure that every column mapping exists in the table
      // The converse is not required -- some database columns may not be necessary
      List<String> missingColumns = Lists.newArrayList();
      for (String col : columnMappings.keySet()) {
        if (!dbColNames.contains(col)) {
          missingColumns.add(col);
          success = false;
        }
      }

      if (!success && !missingColumns.isEmpty()) {
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), "columnNames", Errors.JDBCDEST_04,
            "Missing required columns " + Joiner.on(", ").join(missingColumns)));
      }
    } catch (Exception e) {
      LOG.debug("Exception while validating column mapping", e);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), "columnNames", Errors.JDBCDEST_04, e.toString()));
      success = false;
    }

    return success;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Batch batch) throws StageException {
    try {
      try {
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        // TODO: Enable statement caching -- likely requires database-specific code
        statement = connection.prepareStatement(queryString);
      } catch (SQLException e) {
        logSQLException(e);
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            return;
          case TO_ERROR:
            Iterator<Record> failedRecords = batch.getRecords();
            while (failedRecords.hasNext()) {
              final Record record = failedRecords.next();
              getContext().toError(record, Errors.JDBCDEST_06, e.toString(), e);
            }
            return;
          case STOP_PIPELINE:
            throw new StageException(Errors.JDBCDEST_06, e.toString(), e);
          default:
            throw new IllegalStateException(
                Utils.format("It should never happen. OnError '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }

      // The batch holding the current batch to INSERT.
      Iterator<Record> records = batch.getRecords();

      while (records.hasNext()) {
        final Record record = records.next();

        try {
          int i = 1;
          for (String fieldName : columnMappings.values()) {
            Field field = record.get(fieldName);
            if (null == field) {
              throw new OnRecordErrorException(Errors.JDBCDEST_04, "Missing field in record: " + fieldName);
            }
            final Field.Type fieldType = field.getType();
            final Object value = field.getValue();
            // Special cases for handling SDC Lists and Maps,
            // basically unpacking them into raw types.
            switch (fieldType) {
              case LIST:
                List<Object> unpackedList = new ArrayList<>();
                for (Field item : (List<Field>) value) {
                  unpackedList.add(item.getValue());
                }
                Array array = connection.createArrayOf(getSQLTypeName(fieldType), unpackedList.toArray());
                statement.setArray(i, array);
                break;
              case MAP:
                throw new OnRecordErrorException(Errors.JDBCDEST_05, field.getType());
              case DATE:
              case DATETIME:
                // Java Date types are not accepted by JDBC drivers, so we need to convert ot java.sql.Date to be safe
                java.util.Date date = field.getValueAsDate();
                statement.setObject(i, new java.sql.Date(date.getTime()));
                break;
              default:
                statement.setObject(i, value);
                break;
            }

            i++;
          }
          statement.addBatch();
        } catch (OnRecordErrorException | SQLException e) {
          LOG.debug("Error while adding batch", e);
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().toError(record, Errors.JDBCDEST_02, record.getHeader().getSourceId(), e.toString(), e);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.JDBCDEST_02, record.getHeader().getSourceId(), e.toString(), e);
            default:
              throw new IllegalStateException(
                  Utils.format("It should never happen. OnError '{}'", getContext().getOnErrorRecord(), e)
              );
          }
        }
      }

      try {
        statement.executeBatch();
        connection.commit();
      } catch (SQLException e) {
        logSQLException(e);
        if (rollbackOnError) {
          doRollback(connection);
        } else {
          doCommit(connection);
        }
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            Iterator<Record> failedRecords = batch.getRecords();
            // Some databases drivers allow us to figure out which record in a particular batch failed.
            // In the case that we have a list of update counts, we can mark just the record as erroneous.
            // In the worst case, we just send the entire batch to error, and we'll have to come up with a
            // a better way of dealing with this.
            if (e instanceof BatchUpdateException && ((BatchUpdateException) e).getUpdateCounts().length > 0) {
              BatchUpdateException bue = (BatchUpdateException) e;

              int i = 0;
              while (failedRecords.hasNext()) {
                final Record record = failedRecords.next();
                if (bue.getUpdateCounts()[i] == PreparedStatement.EXECUTE_FAILED) {
                  getContext().toError(record, Errors.JDBCDEST_02, record.getHeader().getSourceId(), e.toString(), e);
                }
                i++;
              }
            } else {
              // Just invalidate all of them
              while (failedRecords.hasNext()) {
                final Record record = failedRecords.next();
                getContext().toError(record, Errors.JDBCDEST_02, record.getHeader().getSourceId(), e.toString(), e);
              }
            }
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.JDBCDEST_02, e);
          default:
            throw new IllegalStateException(
                Utils.format("It should never happen. OnError '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }
    } finally {
      closeQuietly(statement);
      closeQuietly(connection);
    }
  }

  private void setColumnMappings() {
    columnMappings = new LinkedHashMap<>();
    for (JdbcFieldMappingConfig column : columnNames) {
      columnMappings.put(column.columnName.toLowerCase(), column.field.toLowerCase());
    }
  }

  static void logSQLException(SQLException e) {
    LOG.error("SQLException: {}", e.toString());
    SQLException next = e.getNextException();
    if (null != next) {
      logSQLException(e.getNextException());
    }
  }

  // This is necessary for supporting array datatypes. For some awful reason, the JDBC
  // spec requires a string name for a datatype, rather than just an enum.
  static String getSQLTypeName(Field.Type type) throws OnRecordErrorException {
    switch (type) {
      case BOOLEAN:
        return "BOOLEAN";
      case CHAR:
        return "CHAR";
      case BYTE:
        return "BINARY";
      case SHORT:
        return "SMALLINT";
      case INTEGER:
        return "INTEGER";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case DATE:
        return "DATE";
      case DATETIME:
        return "TIMESTAMP";
      case DECIMAL:
        return "DECIMAL";
      case STRING:
        return "VARCHAR";
      case BYTE_ARRAY:
        return "VARBINARY";
      case MAP:
        throw new OnRecordErrorException(Errors.JDBCDEST_05, "Unsupported list or map type: MAP");
      case LIST:
        return "ARRAY";
    }

    throw new OnRecordErrorException(Errors.JDBCDEST_05, "Unsupported type: " + type.name());
  }

  private void doRollback(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      logSQLException(e);
    }
  }

  private void doCommit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
      logSQLException(e);
    }
  }
}