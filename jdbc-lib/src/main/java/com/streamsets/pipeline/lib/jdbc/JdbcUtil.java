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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.MILLISECONDS;

/**
 * Utility classes for working with JDBC
 */
public class JdbcUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);
  /**
   * Position in ResultSet for column and primary key metadata of the column name.
   *
   * @see java.sql.DatabaseMetaData#getColumns
   * @see java.sql.DatabaseMetaData#getPrimaryKeys
   */
  private static final int COLUMN_NAME = 4;
  private static final String EL_PREFIX = "${";
  private static final String CUSTOM_MAPPINGS = "columnNames";

  public static final String TABLE_NAME = "tableNameTemplate";

  private JdbcUtil() {
  }

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
  private static final Map<String, String> STANDARD_DATA_ERROR_SQLSTATES = ImmutableMap.of(
      "21", "Cardinality violation",
      "22", "Data exception",
      "23", "Constraint violation",
      "42", "Syntax error or access rule violation",
      "44", "WITH CHECK OPTION violation"
  );

  /**
   * MySQL does not use standard SQL States for some errors
   * handle those as a special case. See MySQL doc:
   * Server Error Codes and Messages
   */
  private static final String MYSQL_GENERAL_ERROR = "HY000";
  private static final Map<String, String> MYSQL_DATA_ERROR_ERROR_CODES = ImmutableMap.of(
      "1364", "Field '%s' doesn't have a default value",
      "1366", "Incorrect %s value: '%s' for column '%s' at row %ld",
      "1391", "Key part '%s' length cannot be 0"
  );

  public static boolean isDataError(String connectionString, SQLException ex) {
    String sqlState = Strings.nullToEmpty(ex.getSQLState());
    String errorCode = String.valueOf(ex.getErrorCode());
    if (sqlState.equals(MYSQL_GENERAL_ERROR) && connectionString.contains(":mysql")) {
      return MYSQL_DATA_ERROR_ERROR_CODES.containsKey(errorCode);
    } else if (sqlState.length() >= 2 && STANDARD_DATA_ERROR_SQLSTATES.containsKey(sqlState.substring(0, 2))) {
      return true;
    }
    return false;
  }

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
   *
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
   *
   * @param connection An open JDBC connection
   * @param tableName table name that is optionally fully qualified with a schema in the form schema.tableName
   * @return ResultSet containing the table metadata for a table
   *
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
   *
   * @param connection An open JDBC connection
   * @param tableName table name that is optionally fully qualified with a schema in the form schema.tableName
   * @return List of primary key column names for a table
   *
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

  public static void setColumnSpecificHeaders(
      Record record,
      ResultSetMetaData metaData,
      String jdbcNameSpacePrefix
  ) throws SQLException {
    Record.Header header = record.getHeader();
    Set<String> tableNames = new HashSet<>();
    for (int i=1; i<=metaData.getColumnCount(); i++) {
      header.setAttribute(jdbcNameSpacePrefix + metaData.getColumnLabel(i) + ".jdbcType", String.valueOf(metaData.getColumnType(i)));

      // Additional headers per various types
      switch(metaData.getColumnType(i)) {
        case Types.DECIMAL:
        case Types.NUMERIC:
          header.setAttribute(jdbcNameSpacePrefix + metaData.getColumnLabel(i) + ".scale", String.valueOf(metaData.getScale(i)));
          header.setAttribute(jdbcNameSpacePrefix + metaData.getColumnLabel(i) + ".precision", String.valueOf(metaData.getPrecision(i)));
          break;
      }

      // Store the column's table name
      tableNames.add(metaData.getTableName(i));
    }

    header.setAttribute(jdbcNameSpacePrefix + "tables", Joiner.on(",").join(tableNames));
  }

  private static String getClobString(Clob data, int maxClobSize) throws IOException, SQLException {
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

  private static byte[] getBlobBytes(Blob data, int maxBlobSize) throws IOException, SQLException {
    if (data == null) {
      return null;
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


  public static Field resultToField(
      ResultSetMetaData md,
      ResultSet rs,
      int columnIndex,
      int maxClobSize,
      int maxBlobSize
  ) throws SQLException, IOException {
      Field field;
      // All types as of JDBC 2.0 are here:
      // https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
      // Good source of recommended mappings is here:
      // http://www.cs.mun.ca/java-api-1.5/guide/jdbc/getstart/mapping.html
      switch (md.getColumnType(columnIndex)) {
        case Types.BIGINT:
          field = Field.create(Field.Type.LONG, rs.getObject(columnIndex));
          break;
        case Types.BINARY:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
          field = Field.create(Field.Type.BYTE_ARRAY, rs.getObject(columnIndex));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          field = Field.create(Field.Type.BOOLEAN, rs.getObject(columnIndex));
          break;
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
          field = Field.create(Field.Type.STRING, rs.getObject(columnIndex));
          break;
        case Types.CLOB:
        case Types.NCLOB:
          field = Field.create(Field.Type.STRING, getClobString(rs.getClob(columnIndex), maxClobSize));
          break;
        case Types.BLOB:
          field = Field.create(Field.Type.BYTE_ARRAY, getBlobBytes(rs.getBlob(columnIndex), maxBlobSize));
          break;
        case Types.DATE:
          field = Field.create(Field.Type.DATE, rs.getDate(columnIndex));
          break;
        case Types.DECIMAL:
        case Types.NUMERIC:
          field = Field.create(Field.Type.DECIMAL, rs.getBigDecimal(columnIndex));
          break;
        case Types.DOUBLE:
          field = Field.create(Field.Type.DOUBLE, rs.getObject(columnIndex));
          break;
        case Types.FLOAT:
        case Types.REAL:
          field = Field.create(Field.Type.FLOAT, rs.getObject(columnIndex));
          break;
        case Types.INTEGER:
          field = Field.create(Field.Type.INTEGER, rs.getObject(columnIndex));
          break;
        case Types.ROWID:
          field = Field.create(Field.Type.STRING, rs.getRowId(columnIndex).toString());
          break;
        case Types.SMALLINT:
        case Types.TINYINT:
          field = Field.create(Field.Type.SHORT, rs.getObject(columnIndex));
          break;
        case Types.TIME:
          field = Field.create(Field.Type.TIME, rs.getObject(columnIndex));
          break;
        case Types.TIMESTAMP:
          field = Field.create(Field.Type.DATETIME, rs.getTimestamp(columnIndex));
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
          return null;
      }

      return field;
  }

  public static LinkedHashMap<String, Field> resultSetToFields(
      ResultSet rs,
      int maxClobSize,
      int maxBlobSize,
      ErrorRecordHandler errorRecordHandler
  ) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>(md.getColumnCount());

    for (int i = 1; i <= md.getColumnCount(); i++) {
      try {
        Field field = resultToField(md, rs, i, maxClobSize, maxBlobSize);

        if (field == null) {
          throw new StageException(JdbcErrors.JDBC_37, md.getColumnType(i), md.getColumnLabel(i));
        }

        fields.put(md.getColumnLabel(i), field);
      } catch (SQLException e) {
        errorRecordHandler.onError(JdbcErrors.JDBC_13, e.getMessage(), e);
      } catch (IOException e) {
        errorRecordHandler.onError(JdbcErrors.JDBC_03, md.getColumnName(i), rs.getObject(i), e);
      }
    }

    return fields;
  }

  public static LinkedHashMap<String, Field> resultSetToFields(
      Record record,
      ResultSet rs,
      int maxClobSize,
      int maxBlobSize,
      List<OnRecordErrorException> errorRecords
      ) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>(md.getColumnCount());

    for (int i = 1; i <= md.getColumnCount(); i++) {
      try {
        Field field = resultToField(md, rs, i, maxClobSize, maxBlobSize);

        if (field == null) {
          throw new StageException(JdbcErrors.JDBC_37, md.getColumnType(i), md.getColumnLabel(i));
        }

        fields.put(md.getColumnLabel(i), field);
      } catch (SQLException e) {
        errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_13,
            md.getColumnName(i), rs.getObject(i)));
      } catch (IOException e) {
        errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_03,
            md.getColumnName(i), rs.getObject(i)));
      }
    }

    return fields;
  }

  public static HikariDataSource createDataSourceForWrite(
      HikariPoolConfigBean hikariConfigBean,
      Properties driverProperties,
      String tableNameTemplate,
      List<Stage.ConfigIssue> issues,
      List<JdbcFieldColumnParamMapping> customMappings,
      Stage.Context context
  ) throws SQLException {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(hikariConfigBean.connectionString);
    config.setUsername(hikariConfigBean.username);
    config.setPassword(hikariConfigBean.password);
    config.setAutoCommit(false);
    config.setReadOnly(false);
    config.setMaximumPoolSize(hikariConfigBean.maximumPoolSize);
    config.setMinimumIdle(hikariConfigBean.minIdle);
    config.setConnectionTimeout(hikariConfigBean.connectionTimeout * MILLISECONDS);
    config.setIdleTimeout(hikariConfigBean.idleTimeout * MILLISECONDS);
    config.setMaxLifetime(hikariConfigBean.maxLifetime * MILLISECONDS);

    if (hikariConfigBean.driverClassName != null && !hikariConfigBean.driverClassName.isEmpty()) {
      config.setDriverClassName(hikariConfigBean.driverClassName);
    }

    if (hikariConfigBean.connectionTestQuery != null && !hikariConfigBean.connectionTestQuery.isEmpty()) {
      config.setConnectionTestQuery(hikariConfigBean.connectionTestQuery);
    }

    // User configurable JDBC driver properties
    config.setDataSourceProperties(driverProperties);

    HikariDataSource dataSource = new HikariDataSource(config);

    // Test connectivity
    Connection connection = dataSource.getConnection();

    // Can only validate schema if the user specified a single table.
    if (!tableNameTemplate.contains(EL_PREFIX)) {
      ResultSet res = JdbcUtil.getTableMetadata(connection, tableNameTemplate);
      if (!res.next()) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), TABLE_NAME, JdbcErrors.JDBC_16, tableNameTemplate));
      } else {
        ResultSet columns = JdbcUtil.getColumnMetadata(connection, tableNameTemplate);
        Set<String> columnNames = new HashSet<>();
        while (columns.next()) {
          columnNames.add(columns.getString(4));
        }
        for (JdbcFieldColumnParamMapping customMapping : customMappings) {
          if (!columnNames.contains(customMapping.columnName)) {
            issues.add(context.createConfigIssue(Groups.JDBC.name(),
                CUSTOM_MAPPINGS,
                JdbcErrors.JDBC_07,
                customMapping.field,
                customMapping.columnName
            ));
          }
        }
      }
    }
    connection.close();

    return dataSource;
  }

  public static HikariDataSource createDataSourceForRead(
      HikariPoolConfigBean hikariConfigBean,
      Properties driverProperties
  ) throws StageException {
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

    HikariDataSource dataSource;
    try {
      dataSource = new HikariDataSource(config);
    } catch (RuntimeException e) {
      LOG.error(JdbcErrors.JDBC_06.getMessage(), e);
      throw new StageException(JdbcErrors.JDBC_06, e.getCause().toString());
    }

    return dataSource;
  }

  public static void closeQuietly(AutoCloseable c) {
    try {
      if (null != c) {
        c.close();
      }
    } catch (Exception ignored) {
    }
  }

  public static void write(
      Batch batch,
      ELEval tableNameEval,
      ELVars tableNameVars,
      String tableNameTemplate,
      LoadingCache<String, JdbcRecordWriter> recordWriters,
      ErrorRecordHandler errorRecordHandler
  ) throws StageException {
    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(
        tableNameEval,
        tableNameVars,
        tableNameTemplate,
        batch
    );
    Set<String> tableNames = partitions.keySet();
    for (String tableName : tableNames) {
      List<OnRecordErrorException> errors = recordWriters.getUnchecked(tableName).writeBatch(partitions.get(tableName));
      for (OnRecordErrorException error : errors) {
        errorRecordHandler.onError(error);
      }
    }
  }
}
