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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.jdbc.Errors;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcFieldMappingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class JdbcBaseRecordWriter implements JdbcRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcBaseRecordWriter.class);
  private final List<JdbcFieldMappingConfig> customMappings;

  private DataSource dataSource;
  private String tableName;
  private boolean rollbackOnError;

  private Map<String, String> columnsToFields = new HashMap<>();

  public JdbcBaseRecordWriter(
      DataSource dataSource,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldMappingConfig> customMappings
  ) throws StageException {
    this.dataSource = dataSource;
    this.tableName = tableName;
    this.rollbackOnError = rollbackOnError;
    this.customMappings = customMappings;

    createDefaultFieldMappings();
    createCustomFieldMappings();
  }

  protected void createDefaultFieldMappings() throws StageException {
    try {
      Connection connection = dataSource.getConnection();
      ResultSet columns = JdbcUtil.getColumnMetadata(connection, tableName);
      while (columns.next()) {
        String columnName = columns.getString(4);
        columnsToFields.put(columnName, "/" + columnName); // Default implicit field mappings
      }
    } catch (SQLException e) {
      String errorMessage = JdbcUtil.formatSqlException(e);
      LOG.error(errorMessage);
      throw new StageException(Errors.JDBCDEST_09, tableName);
    }
  }

  protected void createCustomFieldMappings() {
    for (JdbcFieldMappingConfig mapping : customMappings) {
      LOG.debug("Custom mapping field {} to column {}", mapping.field, mapping.columnName);
      if (columnsToFields.containsKey(mapping.columnName)) {
        LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
        columnsToFields.put(mapping.columnName, mapping.field);
      }
    }
  }

  // This is necessary for supporting array data types. For some awful reason, the JDBC
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
  protected DataSource getDataSource() {
    return dataSource;
  }

  /**
   * SQL Table to SDC Field mappings
   * @return map of the mappings
   */
  protected Map<String, String> getColumnsToFields() {
    return columnsToFields;
  }

  /**
   * Whether or not to try to perform a transaction rollback on error.
   * @return whether to rollback the transaction
   */
  protected boolean getRollbackOnError() {
    return rollbackOnError;
  }
}
