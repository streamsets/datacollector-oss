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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class JdbcBaseRecordWriter implements JdbcRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcBaseRecordWriter.class);
  private final List<JdbcFieldColumnParamMapping> customMappings;

  private String connectionString;
  private DataSource dataSource;
  private String tableName;
  private boolean rollbackOnError;

  private Map<String, String> columnsToFields = new HashMap<>();
  private Map<String, String> columnsToParameters = new HashMap<>();
  private final List<JdbcFieldColumnMapping> generatedColumnMappings;
  private Map<String, Integer> columnType = new HashMap<>();

  public JdbcBaseRecordWriter(
      String connectionString,
      DataSource dataSource,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldColumnParamMapping> customMappings
  ) throws StageException {
    this(connectionString, dataSource, tableName, rollbackOnError, customMappings, null);
  }

  public JdbcBaseRecordWriter(
      String connectionString,
      DataSource dataSource,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldColumnParamMapping> customMappings,
      List<JdbcFieldColumnMapping> generatedColumnMappings
  ) throws StageException {
    this.connectionString = connectionString;
    this.dataSource = dataSource;
    this.tableName = tableName;
    this.rollbackOnError = rollbackOnError;
    this.customMappings = customMappings;
    this.generatedColumnMappings = generatedColumnMappings;

    createDefaultFieldMappings();
    createCustomFieldMappings();

  }

  int getColumnType(String columnName) { return columnType.get(columnName); }

  private void createDefaultFieldMappings() throws StageException {
    try (Connection connection = dataSource.getConnection()) {
      try (ResultSet columns = JdbcUtil.getColumnMetadata(connection, tableName)) {
        while (columns.next()) {
          String columnName = columns.getString(4);
          columnsToFields.put(columnName, "/" + columnName); // Default implicit field mappings
          columnsToParameters.put(columnName, "?");
          columnType.put(columnName, columns.getInt(5));
        }
      }
    } catch (SQLException e) {
      String errorMessage = JdbcUtil.formatSqlException(e);
      LOG.error(errorMessage);
      LOG.debug(errorMessage, e);
      throw new StageException(JdbcErrors.JDBC_09, tableName);
    }
  }

  private void createCustomFieldMappings() {
    for (JdbcFieldColumnParamMapping mapping : customMappings) {
      LOG.debug("Custom mapping field {} to column {}", mapping.field, mapping.columnName);
      if (columnsToFields.containsKey(mapping.columnName)) {
        LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
        columnsToFields.put(mapping.columnName, mapping.field);
        columnsToParameters.put(mapping.columnName, mapping.paramValue);
      }
    }
  }

  // This is necessary for supporting array data types. For some awful reason, the JDBC
  // spec requires a string name for a data type, rather than just an enum.
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
      case TIME:
        return "TIME";
      case DATETIME:
        return "TIMESTAMP";
      case DECIMAL:
        return "DECIMAL";
      case STRING:
        return "VARCHAR";
      case BYTE_ARRAY:
        return "VARBINARY";
      case LIST_MAP:
      case MAP:
        throw new OnRecordErrorException(JdbcErrors.JDBC_05, "Unsupported list or map type: MAP");
      case LIST:
        return "ARRAY";
      default:
        throw new OnRecordErrorException(JdbcErrors.JDBC_05, "Unsupported type: " + type.name());
    }
  }

  /**
   * Database connection string
   * @return connection string
   */
  protected String getConnectionString() {
    return connectionString;
  }

  /**
   * Table this writer will write to.
   * @return table name
   */
  protected String getTableName() {
    return tableName;
  }

  /**
   * JDBC DataSource used for writing.
   * @return JDBC DataSource
   */
  DataSource getDataSource() {
    return dataSource;
  }

  /**
   * SQL Table to SDC Field mappings
   * @return map of the mappings
   */
  Map<String, String> getColumnsToFields() {
    return columnsToFields;
  }

  /**
   * SQL Columns to custom parameter mappings. These will be used to parameterized
   * an INSERT statement
   * @return map of the mappings
   */
  Map<String, String> getColumnsToParameters() {
    return columnsToParameters;
  }

  /**
   * Whether or not to try to perform a transaction rollback on error.
   * @return whether to rollback the transaction
   */
  boolean getRollbackOnError() {
    return rollbackOnError;
  }

  static List<Object> unpackList(List<Field> value) {
    List<Object> unpackedList = new ArrayList<>();
    for (Field item : value) {
      unpackedList.add(item.getValue());
    }
    return unpackedList;
  }

  List<JdbcFieldColumnMapping> getGeneratedColumnMappings() {
    return generatedColumnMappings;
  }

  void writeGeneratedColumns(
      PreparedStatement statement,
      Iterator<Record> iter,
      List<OnRecordErrorException> errorRecords
  ) throws SQLException {
    ResultSet resultSet = statement.getGeneratedKeys();

    ResultSetMetaData md = resultSet.getMetaData();
    int numColumns = md.getColumnCount();

    while (resultSet.next()) {
      Record record = iter.next();
      // Process row
      for (int i = 1; i <= numColumns; i++) {
        try {
          // Assuming generated columns can't be CLOBs/BLOBs, so just pass
          // zero for maxClobSize
          Field field = JdbcUtil.resultToField(md, resultSet, i, 0, 0);

          if (field == null) {
            LOG.error(JdbcErrors.JDBC_03.getMessage(), md.getColumnName(i), resultSet.getObject(i));
            errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_03,
                md.getColumnName(i), resultSet.getObject(i)));
          }

          record.set(generatedColumnMappings.get(i - 1).field, field);
        } catch (IOException e) {
          LOG.error(JdbcErrors.JDBC_03.getMessage(), md.getColumnName(i), resultSet.getObject(i));
          errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_03,
              md.getColumnName(i), resultSet.getObject(i)));
        }
      }
    }
  }
}
