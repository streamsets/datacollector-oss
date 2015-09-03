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

import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility classes for working with JDBC
 */
public class JdbcUtil {
  /**
   * Position in ResultSet for column and primary key metadata of the column name.
   *
   * @see java.sql.DatabaseMetaData#getColumns
   * @see java.sql.DatabaseMetaData#getPrimaryKeys
   */
  public static final int COLUMN_NAME = 4;

  /**
   * <p>Mapping of sqlStates that when encountered should determine that we will send a record to the
   * error pipeline. All other SQL states will result in a StageException.
   * </p>
   * <p>
   * Errors that result in the record to error pipeline should generally be due to invalid data.
   * Other exceptions are either an error in our system or the database, and should cause a StageException.
   * </p>
   * <p>
   * To minimize the initial size of this mapping, SqlState error classes are listed here and not the full error
   * codes as there are many.
   * </p>
   */
  public static final Map<String, String> SQLSTATE_TO_ERROR = ImmutableMap.of(
      "21", "Cardinality violation",
      "22", "Data exception",
      "23", "Constraint violation",
      "42", "Syntax error or access rule violation",
      "44", "WITH CHECK OPTION violation"
  );

  /**
   * Formats the error message of a {@link java.sql.SQLException} for human consumption.
   *
   * @param ex SQLException
   * @return Formatted string with database-specific error code, error message, and SQLState
   */
  public static String formatSqlException(SQLException ex) {
    StringBuilder sb = new StringBuilder();
    for (Throwable e : ex) {
      if (e instanceof SQLException) {
        sb.append("SQLState: " + ((SQLException) e).getSQLState() + "\n")
            .append("Error Code: " + ((SQLException) e).getErrorCode() + "\n")
            .append("Message: " + e.getMessage() + "\n");
        Throwable t = ex.getCause();
        while (t != null) {
          sb.append("Cause: " + t + "\n");
          t = t.getCause();
        }
      }
    }
    return sb.toString();
  }

  /**
   * Wrapper for {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String)} that detects
   * the format of the supplied tableName.
   *
   * @param connection An open JDBC connection
   * @param tableName table name that is optionally fully qualified with a schema in the form schema.tableName
   * @return ResultSet containing the column metadata
   * @throws SQLException
   */
  public static ResultSet getColumnMetadata(Connection connection, String tableName) throws SQLException {
    String table = tableName;
    String schema = null;
    DatabaseMetaData metadata = connection.getMetaData();
    if (tableName.contains(".")) {
      // Need to split this into the schema and table parts for column metadata to be retrieved.
      String[] parts = tableName.split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException();
      }
      schema = parts[0];
      table = parts[1];
    }
    return metadata.getColumns(null, schema, table, null); // Get all columns for this table
  }

  /**
   * Wrapper for {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}
   * @param connection An open JDBC connection
   * @param tableName table name that is optionally fully qualified with a schema in the form schema.tableName
   * @return ResultSet containing the table metadata for a table
   * @throws SQLException
   */
  public static ResultSet getTableMetadata(Connection connection, String tableName) throws SQLException {
    String table = tableName;
    String schema = null;
    DatabaseMetaData metadata = connection.getMetaData();
    if (tableName.contains(".")) {
      // Need to split this into the schema and table parts for column metadata to be retrieved.
      String[] parts = tableName.split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException();
      }
      schema = parts[0];
      table = parts[1];
    }
    return metadata.getTables(null, schema, table, new String[]{"TABLE"});
  }

  /**
   * Wrapper for {@link java.sql.DatabaseMetaData#getPrimaryKeys(String, String, String)}
   * @param connection An open JDBC connection
   * @param tableName table name that is optionally fully qualified with a schema in the form schema.tableName
   * @return List of primary key column names for a table
   * @throws SQLException
   */
  public static List<String> getPrimaryKeys(Connection connection, String tableName) throws SQLException {
    String table = tableName;
    String schema = null;
    DatabaseMetaData metadata = connection.getMetaData();
    if (tableName.contains(".")) {
      // Need to split this into the schema and table parts for column metadata to be retrieved.
      String[] parts = tableName.split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException();
      }
      schema = parts[0];
      table = parts[1];
    }

    List<String> keys = new ArrayList<>();
    ResultSet result = metadata.getPrimaryKeys(null, schema, table);
    while (result.next()) {
      keys.add(result.getString(COLUMN_NAME));
    }
    return keys;
  }
}
