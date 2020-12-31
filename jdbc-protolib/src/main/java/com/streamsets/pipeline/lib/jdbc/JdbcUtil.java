/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import microsoft.sql.DateTimeOffset;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  /**
   * Column name for extracting table name for imported keys
   *
   * @see java.sql.DatabaseMetaData#getImportedKeys(String, String, String)
   */
  private static final String PK_TABLE_NAME = "PKTABLE_NAME";

  public static final String TABLE_NAME = "tableNameTemplate";
  public static final String SCHEMA_NAME = "schema";

  /**
   * List of RDBMs that do not differentiate between databases and schemas.
   * Use lower-case for any new value.
   */
  private static final String[] RDBMS_WITHOUT_SCHEMAS = {"mysql", "mariadb", "memsql", "teradata"};

  /**
   * Parameterized SQL statements to the database.
   *
   * @see java.sql.Connection#prepareStatement
   */
  private static final Joiner joiner = Joiner.on(", ");
  private static final Joiner joinerColumn = Joiner.on(" = ?, ");
  private static final Joiner joinerWhereClause =  Joiner.on(" = ? AND ");
  private static final Joiner joinerWithQuote = Joiner.on("\", \"");
  private static final Joiner joinerColumnWithQuote = Joiner.on("\" = ?, \"");
  private static final Joiner joinerWhereClauseWitheQuote =  Joiner.on("\" = ? AND \"");

  private static final String[] METADATA_TABLE_TYPE = new String[]{"TABLE"};
  private static final String[] METADATA_TABLE_VIEW_TYPE = new String[]{"TABLE", "VIEW"};

  /**
   * The query to select the min value for a particular offset column
   */
  public static final String MIN_OFFSET_VALUE_QUERY = "SELECT MIN(%s) FROM %s";

  /**
   * The query to select the max value for a particular offset column
   */
  public static final String MAX_OFFSET_VALUE_QUERY = "SELECT MAX(%s) FROM %s";

  /**
   * The index within the result set for the column that contains the min or max offset value
   */
  private static final int MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX = 1;

  public static final int NANOS_TO_MILLIS_ADJUSTMENT = 1_000_000;
  public static final String FIELD_ATTRIBUTE_NANOSECONDS = "nanoSeconds";

  public JdbcUtil() {
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
   * Oracle specific SQL States: https://docs.oracle.com/cd/E15817_01/appdev.111/b31228/appd.htm
   */
  private static final Set<String> ORACLE_DATA_SQLSTATES = ImmutableSet.of(
    "72000"
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

  public static String[] getMetadataTableViewType() {
    return METADATA_TABLE_VIEW_TYPE;
  }

  public boolean isDataError(List<String> customDataSqlCodes, String connectionString, SQLException ex) {
    String sqlState = Strings.nullToEmpty(ex.getSQLState());
    String errorCode = String.valueOf(ex.getErrorCode());

    while (StringUtils.isEmpty(sqlState) && ex.getNextException() != null) {
      ex = ex.getNextException();
      sqlState= Strings.nullToEmpty(ex.getSQLState());
      errorCode = String.valueOf(ex.getErrorCode());
    }

    if(customDataSqlCodes.contains(sqlState)) {
      return true;
    } else if (sqlState.equals(MYSQL_GENERAL_ERROR) && connectionString.contains(":mysql")) {
      return MYSQL_DATA_ERROR_ERROR_CODES.containsKey(errorCode);
    } else if (connectionString.contains(":oracle:") && ORACLE_DATA_SQLSTATES.contains(sqlState)) {
      return true;
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
  public String formatSqlException(SQLException ex) {
    StringBuilder sb = new StringBuilder();
    Set<String> messages = new HashSet<>();
    for (Throwable e : ex) {
      if (e instanceof SQLException) {
        String message = e.getMessage();
        if (!messages.add(message)) {
          continue;
        }
        sb.append("SQLState: " + ((SQLException) e).getSQLState() + "\n")
            .append("Error Code: " + ((SQLException) e).getErrorCode() + "\n")
            .append("Message: " + message + "\n");
        Throwable t = ex.getCause();
        while (t != null) {
          if (messages.add(t.getMessage())) {
            sb.append("Cause: " + t + "\n");
          }
          t = t.getCause();
        }
      }
    }
    return sb.toString();
  }

  /**
   * Wrapper for {@link Connection#getCatalog()} to solve problems with RDBMs for which catalog and schema are the same
   * thing. For these RDBMs, it returns the schema name when it is not-null and not-empty; otherwise, it returns the
   * value of java.sql.Connection.getCatalog(). For other RDBMs, it returns always the value of
   * java.sql.Connection.getCatalog().
   *
   * @param connection An open JDBC connection
   * @param schema The schema name we want to use
   * @return The current catalog or the schema name passed as argument
   * @throws SQLException
   */
  private String getCatalog(Connection connection, String schema) throws SQLException {
    if (Strings.isNullOrEmpty(schema)) {
      return connection.getCatalog();
    }

    String name = connection.getMetaData().getDatabaseProductName().toLowerCase();

    for (String d : RDBMS_WITHOUT_SCHEMAS) {
      if (name.contains(d)) {
        return schema;
      }
    }

    return connection.getCatalog();
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
  public ResultSet getColumnMetadata(Connection connection, String schema, String tableName) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    // Get all columns for this table
    return metadata.getColumns(getCatalog(connection, schema), schema, tableName, null);
  }

  /**
   * Wrapper for {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}
   *
   * @param connection open JDBC connection
   * @param schema schema name, can be null
   * @param tableName table name or pattern, optionally fully qualified in the form schema.tableName
   * @return ResultSet containing the table and view metadata
   *
   * @throws SQLException
   */
  public ResultSet getTableAndViewMetadata(
      Connection connection,
      String schema,
      String tableName
  ) throws SQLException {
    return connection.getMetaData().getTables(
        getCatalog(connection, schema),
        schema,
        tableName,
        METADATA_TABLE_VIEW_TYPE
    );
  }

  /**
   * Wrapper for {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}
   *
   * @param connection open JDBC connection
   * @param schema schema name, can be null
   * @param tableName table name or pattern, optionally fully qualified in the form schema.tableName
   * @return ResultSet containing the table metadata
   *
   * @throws SQLException
   */
  public ResultSet getTableMetadata(Connection connection, String schema, String tableName) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    return metadata.getTables(getCatalog(connection, schema), schema, tableName, METADATA_TABLE_TYPE);
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
  public List<String> getPrimaryKeys(Connection connection, String schema, String tableName) throws SQLException {
    String table = tableName;
    DatabaseMetaData metadata = connection.getMetaData();
    List<String> keys = new ArrayList<>();
    try (ResultSet result = metadata.getPrimaryKeys(getCatalog(connection, schema), schema, table)) {
      while (result.next()) {
        keys.add(result.getString(COLUMN_NAME));
      }
    }
    return keys;
  }

  public static Map<String, String> getMinimumOffsetValues(
      DatabaseVendor vendor,
      Connection connection,
      String schema,
      String tableName,
      QuoteChar quoteChar,
      Collection<String> offsetColumnNames
  ) throws SQLException {
    return getMinMaxOffsetValueHelper(
        MIN_OFFSET_VALUE_QUERY,
        vendor,
        connection,
        schema,
        tableName,
        quoteChar,
        offsetColumnNames
    );
  }

  public static Map<String, String> getMaximumOffsetValues(
      DatabaseVendor vendor,
      Connection connection,
      String schema,
      String tableName,
      QuoteChar quoteChar,
      Collection<String> offsetColumnNames
  ) throws SQLException {
    return getMinMaxOffsetValueHelper(
        MAX_OFFSET_VALUE_QUERY,
        vendor,
        connection,
        schema,
        tableName,
        quoteChar,
        offsetColumnNames
    );
  }

  private static long getEpochMillisFromSqlDate(java.sql.Date date) {
    return date.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
  }

  private static long getEpochMillisFromSqlTime(java.sql.Time time) {
    return time.toLocalTime().toSecondOfDay() * 1000L;
  }

  private static Map<String, String> getMinMaxOffsetValueHelper(
      String minMaxQuery,
      DatabaseVendor vendor,
      Connection connection,
      String schema,
      String tableName,
      QuoteChar quoteChar,
      Collection<String> offsetColumnNames
  ) throws SQLException {
    Map<String, String> minMaxOffsetValues = new HashMap<>();
    final String qualifiedTableName = TableContextUtil.getQuotedQualifiedTableName(
        schema,
        tableName,
        quoteChar.getQuoteCharacter()
    );
    for (String offsetColumn : offsetColumnNames) {
      String quoteCharLeft = quoteChar.getQuoteCharacter();
      String quoteCharRight = quoteChar.getQuoteCharacter().equals("[") ? "]" : quoteChar.getQuoteCharacter();
      final String qualifiedOffsetColumn = TableContextUtil.getQuotedObjectName(offsetColumn, quoteCharLeft, quoteCharRight);
      final String minMaxOffsetQuery = String.format(minMaxQuery, qualifiedOffsetColumn, qualifiedTableName);
      LOG.debug("Issuing {} offset query: {}",
            minMaxQuery.equals(MIN_OFFSET_VALUE_QUERY) ? "MINIMUM" : "MAXIMUM", minMaxOffsetQuery);
      try (
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(minMaxOffsetQuery)
      ) {
        if (rs.next()) {
          String minMaxValue = null;
          final int colType = rs.getMetaData().getColumnType(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX);

          switch (vendor) {
            case ORACLE:
              if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.ORACLE).contains(colType)) {
                switch (colType) {
                  case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                  case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE:
                    OffsetDateTime offsetDateTime = rs.getObject(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX, OffsetDateTime.class);
                    if(offsetDateTime != null) {
                      minMaxValue = offsetDateTime.toZonedDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    }
                    break;
                  default:
                    throw new IllegalStateException(Utils.format("Unexpected type: {}", colType));
                }
              }
              break;

            case SQL_SERVER:
              if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.SQL_SERVER).contains(colType)) {
                if (colType == TableContextUtil.TYPE_SQL_SERVER_DATETIMEOFFSET) {
                  DateTimeOffset dateTimeOffset = rs.getObject(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX, DateTimeOffset.class);
                  if (dateTimeOffset != null) {
                    minMaxValue = dateTimeOffset.getOffsetDateTime().toZonedDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                  }
                }
              }
              break;
          }

          if(minMaxValue == null) {
            switch (colType) {
              case Types.DATE:
                java.sql.Date date = rs.getDate(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX);
                if (date != null) {
                  minMaxValue = String.valueOf(
                      getEpochMillisFromSqlDate(date)
                  );
                }
                break;
              case Types.TIME:
                java.sql.Time time = rs.getTime(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX);
                if (time != null) {
                  minMaxValue = String.valueOf(
                      getEpochMillisFromSqlTime(time)
                  );
                }
                break;
              case Types.TIMESTAMP:
                Timestamp timestamp = rs.getTimestamp(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX);
                if (timestamp != null) {
                  final Instant instant = timestamp.toInstant();
                  minMaxValue = String.valueOf(instant.toEpochMilli());
                }
                break;
              default:
                minMaxValue = rs.getString(MIN_MAX_OFFSET_VALUE_QUERY_RESULT_SET_INDEX);
                break;
            }
          }
          if (minMaxValue != null) {
            minMaxOffsetValues.put(offsetColumn, minMaxValue);
          }
        } else {
          LOG.warn("Unable to get minimum offset value using query {}; result set had no rows", minMaxOffsetQuery);
        }
      }
    }

    return minMaxOffsetValues;
  }

  /**
   * Wrapper for {@link java.sql.DatabaseMetaData#getImportedKeys(String, String, String)}
   *
   * @param connection An open JDBC connection
   * @param tableName table name that is optionally fully qualified with a schema in the form schema.tableName
   * @return List of Table Names whose primary key are referred as foreign key by the table tableName
   *
   * @throws SQLException
   */
  public Set<String> getReferredTables(Connection connection, String schema, String tableName) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();

    ResultSet result = metadata.getImportedKeys(getCatalog(connection, schema), schema, tableName);
    Set<String> referredTables = new HashSet<>();
    while (result.next()) {
      referredTables.add(result.getString(PK_TABLE_NAME));
    }
    return referredTables;
  }

  public void setColumnSpecificHeaders(
      Record record,
      Set<String> knownTableNames,
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

      String tableName = metaData.getTableName(i);

      // Store the column's table name (if not empty)
      if (StringUtils.isNotEmpty(tableName)) {
        tableNames.add(tableName);
      }
    }

    if (tableNames.isEmpty()) {
      tableNames.addAll(knownTableNames);
    }

    header.setAttribute(jdbcNameSpacePrefix + "tables", Joiner.on(",").join(tableNames));
  }

  private String getClobString(Clob data, int maxClobSize) throws IOException, SQLException {
    if (data == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    int bufLen = 1024;
    char[] cbuf = new char[bufLen];

    // Read up to max clob length
    long maxRemaining = maxClobSize;
    int count;
    try(Reader r = data.getCharacterStream()) {
      while ((count = r.read(cbuf)) > -1 && maxRemaining > 0) {
        // If c is more then the remaining chars we want to read, read only as many are available
        if (count > maxRemaining) {
          count = (int) maxRemaining;
        }
        sb.append(cbuf, 0, count);
        // decrement available according to the number of chars we've read
        maxRemaining -= count;
      }
    }
    return sb.toString();
  }

  private byte[] getBlobBytes(Blob data, int maxBlobSize) throws IOException, SQLException {
    if (data == null) {
      return null;
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    int bufLen = 1024;
    byte[] buf = new byte[bufLen];

    // Read up to max blob length
    long maxRemaining = maxBlobSize;
    int count;
    try(InputStream is = data.getBinaryStream()) {
      while ((count = is.read(buf)) > -1 && maxRemaining > 0) {
        // If count is more then the remaining bytes we want to read, read only as many are available
        if (count > maxRemaining) {
          count = (int) maxRemaining;
        }
        os.write(buf, 0, count);
        // decrement available according to the number of bytes we've read
        maxRemaining -= count;
      }
    }
    return os.toByteArray();
  }

  public Field resultToField(
      ResultSetMetaData md,
      ResultSet rs,
      int columnIndex,
      int maxClobSize,
      int maxBlobSize,
      UnknownTypeAction unknownTypeAction
  ) throws SQLException, IOException, StageException {
    return resultToField(
      md,
      rs,
      columnIndex,
      maxClobSize,
      maxBlobSize,
      DataType.USE_COLUMN_TYPE,
      unknownTypeAction,
      false,
      DatabaseVendor.UNKNOWN
    );
  }

  public Field resultToField(
      ResultSetMetaData md,
      ResultSet rs,
      int columnIndex,
      int maxClobSize,
      int maxBlobSize,
      DataType userSpecifiedType,
      UnknownTypeAction unknownTypeAction,
      boolean timestampToString,
      DatabaseVendor vendor
  ) throws SQLException, IOException, StageException {
      Field field;
      if (userSpecifiedType != DataType.USE_COLUMN_TYPE) {
        // If user specifies the data type, overwrite the column type returned by database.
        field = Field.create(Field.Type.valueOf(userSpecifiedType.getLabel()), rs.getObject(columnIndex));
      } else {
        // Firstly resolve some vendor specific types - we are careful in case that someone will be clashing
        if(vendor == DatabaseVendor.ORACLE) {
          switch (md.getColumnType(columnIndex)) {
            case TableContextUtil.TYPE_ORACLE_BINARY_FLOAT:
              float floatValue = rs.getFloat(columnIndex);
              return Field.create(Field.Type.FLOAT, rs.wasNull() ? null : floatValue);
            case TableContextUtil.TYPE_ORACLE_BINARY_DOUBLE:
              double doubleValue = rs.getDouble(columnIndex);
              return Field.create(Field.Type.DOUBLE, rs.wasNull() ? null : doubleValue);
            case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE:
            case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              OffsetDateTime offsetDateTime = rs.getObject(columnIndex, OffsetDateTime.class);
              if (offsetDateTime == null) {
                return timestampToString ?
                    Field.create(Field.Type.STRING, null) :
                    Field.create(Field.Type.ZONED_DATETIME, null);
              }
              if (timestampToString) {
                return Field.create(Field.Type.STRING, offsetDateTime.toZonedDateTime().toString());
              }
              // Zoned Datetime can handle high precision
              return Field.create(Field.Type.ZONED_DATETIME, offsetDateTime.toZonedDateTime());
            case Types.SQLXML:
              SQLXML xml = rs.getSQLXML(columnIndex);
              return Field.create(Field.Type.STRING, xml == null ? null : xml.getString());
          }
        } else if (vendor == DatabaseVendor.SQL_SERVER) {
          if (md.getColumnType(columnIndex) == TableContextUtil.TYPE_SQL_SERVER_DATETIMEOFFSET) {
              DateTimeOffset dateTimeOffset = rs.getObject(columnIndex, DateTimeOffset.class);
              if (dateTimeOffset == null) {
                return timestampToString ?
                    Field.create(Field.Type.STRING, null) :
                    Field.create(Field.Type.ZONED_DATETIME, null);
              }
              if (timestampToString) {
                return Field.create(Field.Type.STRING, dateTimeOffset.toString());
              }
              return Field.create(Field.Type.ZONED_DATETIME, dateTimeOffset.getOffsetDateTime().toZonedDateTime());
          }
        } else if (vendor.isOneOf(DatabaseVendor.MYSQL, DatabaseVendor.MARIADB)) {
          // For MySQL we have to deal with unsigned types since the JDBC driver won't transfer them properly for us
          // Tinyint and mediumint (unsigned) are alright since MySQL will auto expand them for purpose of JDBC.
          int columnType = md.getColumnType(columnIndex);
          String columnTypeName = md.getColumnTypeName(columnIndex);
          if(columnType == Types.SMALLINT && columnTypeName.endsWith("UNSIGNED")) {
            int value = rs.getInt(columnIndex);
            return Field.create(Field.Type.INTEGER, rs.wasNull() ? null : value);
          }
          if(columnType == Types.INTEGER && columnTypeName.endsWith("UNSIGNED")) {
            long value = rs.getLong(columnIndex);
            return Field.create(Field.Type.LONG, rs.wasNull() ? null : value);
          }
          if(columnType == Types.BIGINT && columnTypeName.endsWith("UNSIGNED")) {
            BigDecimal value = rs.getBigDecimal(columnIndex);
            return Field.create(Field.Type.DECIMAL, rs.wasNull() ? null : value);
          }
        }

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
            field = Field.create(Field.Type.BYTE_ARRAY, rs.getBytes(columnIndex));
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
            field.setAttribute(HeaderAttributeConstants.ATTR_SCALE, String.valueOf(rs.getMetaData().getScale(columnIndex)));
            field.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, String.valueOf(rs.getMetaData().getPrecision(columnIndex)));
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
            final Timestamp timestamp = rs.getTimestamp(columnIndex);
            if (timestampToString) {
              field = Field.create(Field.Type.STRING, timestamp == null ? null : timestamp.toString());
            } else {
              field = Field.create(Field.Type.DATETIME, timestamp);
              if (timestamp != null) {
                setNanosecondsinAttribute(timestamp.getNanos(), field);
              }
            }
            break;
          // Ugly hack until we can support LocalTime, LocalDate, LocalDateTime, etc.
          case Types.TIME_WITH_TIMEZONE:
            OffsetTime offsetTime = rs.getObject(columnIndex, OffsetTime.class);
            field = Field.create(Field.Type.TIME, Date.from(offsetTime.atDate(LocalDate.MIN).toInstant()));
            break;
          case Types.TIMESTAMP_WITH_TIMEZONE:
            OffsetDateTime offsetDateTime = rs.getObject(columnIndex, OffsetDateTime.class);
            field = Field.create(Field.Type.ZONED_DATETIME, offsetDateTime.toZonedDateTime());
            break;
          //case Types.REF_CURSOR: // JDK8 only
          case Types.SQLXML:
          case Types.STRUCT:
          case Types.ARRAY:
          case Types.DATALINK:
          case Types.DISTINCT:
          case Types.JAVA_OBJECT:
          case Types.NULL:
          case Types.OTHER:
          case Types.REF:
          default:
            if(unknownTypeAction == null) {
              return null;
            }
            switch (unknownTypeAction) {
              case STOP_PIPELINE:
                throw new StageException(JdbcErrors.JDBC_37, md.getColumnType(columnIndex), md.getColumnLabel(columnIndex));
              case CONVERT_TO_STRING:
                Object value = rs.getObject(columnIndex);
                if(value != null) {
                  field = Field.create(Field.Type.STRING, rs.getObject(columnIndex).toString());
                } else {
                  field = Field.create(Field.Type.STRING, null);
                }
                break;
              default:
                throw new IllegalStateException("Unknown action: " + unknownTypeAction);
            }
        }
      }

      return field;
  }

  public static void setNanosecondsinAttribute(int nanoseconds, Field field) {
    final long actualNanos = nanoseconds % NANOS_TO_MILLIS_ADJUSTMENT;
    if (actualNanos > 0) {
      field.setAttribute(FIELD_ATTRIBUTE_NANOSECONDS, String.valueOf(actualNanos));
    }
  }

  public LinkedHashMap<String, Field> resultSetToFields(
      ResultSet rs,
      int maxClobSize,
      int maxBlobSize,
      Map<String, DataType> columnsToTypes,
      ErrorRecordHandler errorRecordHandler,
      UnknownTypeAction unknownTypeAction,
      DatabaseVendor vendor
  ) throws SQLException, StageException {
    return resultSetToFields(
        rs,
        maxClobSize,
        maxBlobSize,
        columnsToTypes,
        errorRecordHandler,
        unknownTypeAction,
        null,
        false,
        vendor
    );
  }

  public LinkedHashMap<String, Field> resultSetToFields(
      ResultSet rs,
      CommonSourceConfigBean commonSourceBean,
      ErrorRecordHandler errorRecordHandler,
      UnknownTypeAction unknownTypeAction,
      DatabaseVendor vendor
  ) throws SQLException, StageException {
    return resultSetToFields(
        rs,
        commonSourceBean.maxClobSize,
        commonSourceBean.maxBlobSize,
        Collections.emptyMap(),
        errorRecordHandler,
        unknownTypeAction,
        null,
        commonSourceBean.convertTimestampToString,
        vendor
    );
  }

  public LinkedHashMap<String, Field> resultSetToFields(
      ResultSet rs,
      CommonSourceConfigBean commonSourceBean,
      ErrorRecordHandler errorRecordHandler,
      UnknownTypeAction unknownTypeAction,
      Set<String> recordHeader,
      DatabaseVendor vendor
  ) throws SQLException, StageException {
    return resultSetToFields(
        rs,
        commonSourceBean.maxClobSize,
        commonSourceBean.maxBlobSize,
        Collections.emptyMap(),
        errorRecordHandler,
        unknownTypeAction,
        recordHeader,
        commonSourceBean.convertTimestampToString,
        vendor
    );
  }

  public LinkedHashMap<String, Field> resultSetToFields(
      ResultSet rs,
      int maxClobSize,
      int maxBlobSize,
      Map<String, DataType> columnsToTypes,
      ErrorRecordHandler errorRecordHandler,
      UnknownTypeAction unknownTypeAction,
      Set<String> recordHeader,
      boolean timestampToString,
      DatabaseVendor vendor
  ) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>(md.getColumnCount());

    for (int i = 1; i <= md.getColumnCount(); i++) {
      try {
        if (recordHeader == null || !recordHeader.contains(md.getColumnName(i))) {
          DataType dataType = columnsToTypes.get(md.getColumnName(i));
          Field field = resultToField(
              md,
              rs,
              i,
              maxClobSize,
              maxBlobSize,
              dataType == null ? DataType.USE_COLUMN_TYPE : dataType,
              unknownTypeAction,
              timestampToString,
              vendor
          );
          fields.put(md.getColumnLabel(i), field);
        }
      } catch (IOException|SQLException e) {
        LOG.debug("Can't read from JDBC: {}", e.getMessage(), e);
        errorRecordHandler.onError(JdbcErrors.JDBC_03, md.getColumnName(i), md.getColumnType(i), rs.getObject(i), e);
      }
    }

    return fields;
  }

  private HikariConfig createDataSourceConfig(
    HikariPoolConfigBean hikariConfigBean,
    boolean autoCommit,
    boolean readOnly
  ) throws StageException {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(hikariConfigBean.getConnectionString());
    if (hikariConfigBean.useCredentials()){
      config.setUsername(hikariConfigBean.getUsername().get());
      config.setPassword(hikariConfigBean.getPassword().get());
    }
    config.setAutoCommit(autoCommit);
    config.setReadOnly(readOnly);
    config.setMaximumPoolSize(hikariConfigBean.maximumPoolSize);
    config.setMinimumIdle(hikariConfigBean.minIdle);
    config.setConnectionTimeout(hikariConfigBean.connectionTimeout * MILLISECONDS);
    config.setIdleTimeout(hikariConfigBean.idleTimeout * MILLISECONDS);
    config.setMaxLifetime(hikariConfigBean.maxLifetime * MILLISECONDS);

    if (!StringUtils.isEmpty(hikariConfigBean.driverClassName)) {
      config.setDriverClassName(hikariConfigBean.driverClassName);
    }

    if (!StringUtils.isEmpty(hikariConfigBean.connectionTestQuery)) {
      config.setConnectionTestQuery(hikariConfigBean.connectionTestQuery);
    }

    if(hikariConfigBean.transactionIsolation != TransactionIsolationLevel.DEFAULT) {
      config.setTransactionIsolation(hikariConfigBean.transactionIsolation.name());
    }

    if(StringUtils.isNotEmpty(hikariConfigBean.initialQuery)) {
      config.setConnectionInitSql(hikariConfigBean.initialQuery);
    }

    config.setDataSourceProperties(hikariConfigBean.getDriverProperties());

    return config;
  }

  public HikariDataSource createDataSourceForWrite(
      HikariPoolConfigBean hikariConfigBean,
      String schemaNameTemplate,
      String tableNameTemplate,
      boolean caseSensitive,
      List<Stage.ConfigIssue> issues,
      List<JdbcFieldColumnParamMapping> customMappings,
      Stage.Context context,
      boolean tableAutoCreate
  ) throws SQLException, StageException {
    HikariDataSource dataSource = new HikariDataSource(createDataSourceConfig(hikariConfigBean,
        hikariConfigBean.isAutoCommit(), false));

    // Can only validate schema+table configuration when the user specified plain constant values and table auto
    // create is not set
    if (isPlainString(schemaNameTemplate) && isPlainString(tableNameTemplate) && !tableAutoCreate) {
      try (
          Connection connection = dataSource.getConnection();
          ResultSet res = getTableMetadata(connection, schemaNameTemplate, tableNameTemplate);
      ) {
        if (!res.next()) {
          issues.add(context.createConfigIssue(Groups.JDBC.name(), TABLE_NAME, JdbcErrors.JDBC_16, tableNameTemplate));
        } else {
          try(ResultSet columns = getColumnMetadata(connection, schemaNameTemplate, tableNameTemplate)) {
            Set<String> columnNames = new HashSet<>();
            while (columns.next()) {
              columnNames.add(columns.getString(4));
            }
            for (JdbcFieldColumnParamMapping customMapping : customMappings) {
              if (customMapping.columnName.isEmpty()) {
                issues.add(context.createConfigIssue(Groups.JDBC.name(),
                    CUSTOM_MAPPINGS,
                    JdbcErrors.JDBC_59
                ));
              }
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
      }
    }

    return dataSource;
  }

  public HikariDataSource createDataSourceForWrite(
      HikariPoolConfigBean hikariConfigBean,
      String schemaNameTemplate,
      String tableNameTemplate,
      boolean caseSensitive,
      List<Stage.ConfigIssue> issues,
      List<JdbcFieldColumnParamMapping> customMappings,
      Stage.Context context
  ) throws SQLException, StageException {
    return createDataSourceForWrite(
        hikariConfigBean,
        schemaNameTemplate,
        tableNameTemplate,
        caseSensitive,
        issues,
        customMappings,
        context,
        false
    );
  }

  public HikariDataSource createDataSourceForRead(
      HikariPoolConfigBean hikariConfigBean
  ) throws StageException {
    HikariDataSource dataSource;
    try {
      dataSource = new HikariDataSource(createDataSourceConfig(
          hikariConfigBean, hikariConfigBean.isAutoCommit(),
          hikariConfigBean.readOnly
      ));
    } catch (RuntimeException e) {
      LOG.error(JdbcErrors.JDBC_06.getMessage(), e);
      throw new StageException(JdbcErrors.JDBC_06, e.toString(), e);
    }

    return dataSource;
  }

  public void closeQuietly(AutoCloseable c) {
    try {
      if (null != c) {
        c.close();
      }
    } catch (Exception ignored) {
    }
  }

  /**
   * Write records to potentially different schemas and tables using EL expressions, and handle errors.
   * @param batch batch of SDC records
   * @param schemaTableClassifier classifier to group records according to the schema and table names, resolving the
   * EL expressions involved.
   * @param recordWriters JDBC record writer cache
   * @param errorRecordHandler error record handler
   * @param perRecord indicate record or batch update
   * @param tableCreator handler which creates the table if it does not exist yet
   * @throws StageException
   */
  public void write(
      Batch batch,
      SchemaTableClassifier schemaTableClassifier,
      LoadingCache<SchemaAndTable, JdbcRecordWriter> recordWriters,
      ErrorRecordHandler errorRecordHandler,
      boolean perRecord,
      JdbcTableCreator tableCreator
  ) throws StageException {
    Multimap<SchemaAndTable, Record> partitions = schemaTableClassifier.classify(batch);

    for (SchemaAndTable key : partitions.keySet()) {
      tableCreator.create(key.getSchemaName(), key.getTableName());
      Iterator<Record> recordIterator = partitions.get(key).iterator();
      write(recordIterator, key, recordWriters, errorRecordHandler, perRecord);
    }
  }

  /**
   * Write records to potentially different schemas and tables using EL expressions, and handle errors.
   *
   * @param batch batch of SDC records
   * @param schemaTableClassifier classifier to group records according to the schema and table names, resolving the
   *     EL expressions involved.
   * @param recordWriters JDBC record writer cache
   * @param errorRecordHandler error record handler
   * @param perRecord indicate record or batch update
   * @throws StageException
   */
  public void write(
      Batch batch,
      SchemaTableClassifier schemaTableClassifier,
      LoadingCache<SchemaAndTable, JdbcRecordWriter> recordWriters,
      ErrorRecordHandler errorRecordHandler,
      boolean perRecord
  ) throws StageException {
    Multimap<SchemaAndTable, Record> partitions = schemaTableClassifier.classify(batch);

    for (SchemaAndTable key : partitions.keySet()) {
      Iterator<Record> recordIterator = partitions.get(key).iterator();
      write(recordIterator, key, recordWriters, errorRecordHandler, perRecord);
    }
  }

  /**
   * Write records to the evaluated tables and handle errors.
   *
   * @param batch batch of SDC records
   * @param tableNameEval table name EL eval
   * @param tableNameVars table name EL vars
   * @param tableNameTemplate table name template
   * @param recordWriters JDBC record writer cache
   * @param errorRecordHandler error record handler
   * @param perRecord indicate record or batch update
   * @throws StageException
   */
  public void write(
      Batch batch,
      ELEval tableNameEval,
      ELVars tableNameVars,
      String tableNameTemplate,
      LoadingCache<String, JdbcRecordWriter> recordWriters,
      ErrorRecordHandler errorRecordHandler,
      boolean perRecord
  ) throws StageException {
    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(
        tableNameEval,
        tableNameVars,
        tableNameTemplate,
        batch
    );
    for (String tableName : partitions.keySet()) {
      Iterator<Record> recordIterator = partitions.get(tableName).iterator();
      write(recordIterator, tableName, recordWriters, errorRecordHandler, perRecord);
    }
  }

  /**
   * Write records to a JDBC destination using the recordWriter specified by key, and handle errors
   *
   * @param recordIterator iterator of SDC records
   * @param key key to select the recordWriter
   * @param recordWriters JDBC record writer cache
   * @param errorRecordHandler error record handler
   * @param perRecord indicate record or batch update
   * @throws StageException
   */
  public <T> void write(
      Iterator<Record> recordIterator,
      T key,
      LoadingCache<T, JdbcRecordWriter> recordWriters,
      ErrorRecordHandler errorRecordHandler,
      boolean perRecord
  ) throws StageException {
    final JdbcRecordWriter jdbcRecordWriter;
    try {
      jdbcRecordWriter = recordWriters.getUnchecked(key);
    } catch (UncheckedExecutionException ex) {
      final Throwable throwable = ex.getCause();
      final ErrorCode errorCode;
      final Object[] messageParams;
      if (throwable instanceof StageException) {
        StageException stageEx = (StageException) ex.getCause();
        errorCode = stageEx.getErrorCode();
        messageParams = stageEx.getParams();
      } else {
        errorCode = JdbcErrors.JDBC_301;
        messageParams = new Object[] {ex.getMessage(), ex.getCause()};
      }
      // Failed to create RecordWriter, report all as error records.
      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();
        errorRecordHandler.onError(new OnRecordErrorException(record, errorCode, messageParams));
      }
      return;
    }
    List<OnRecordErrorException> errors = perRecord
        ? jdbcRecordWriter.writePerRecord(recordIterator)
        : jdbcRecordWriter.writeBatch(recordIterator);

    for (OnRecordErrorException error : errors) {
      errorRecordHandler.onError(error);
    }
  }

  /**
   * Determines whether the actualSqlType is one of the sqlTypes list
   * @param actualSqlType the actual sql type
   * @param sqlTypes arbitrary list of sql types
   * @return true if actual Sql Type is one of the sql Types else false.
   */
  public boolean isSqlTypeOneOf(int actualSqlType, int... sqlTypes) {
    for (int sqlType : sqlTypes) {
      if (sqlType == actualSqlType) {
        return true;
      }
    }
    return false;
  }

  public String generateQuery(
      int opCode,
      String tableName,
      List<String> primaryKeys,
      List<String> primaryKeyParams,
      Map<String, String> columns,
      int numRecords,
      boolean caseSensitive,
      boolean multiRow,
      Record record
  ) throws OnRecordErrorException {
    String query;
    String valuePlaceholder;
    String valuePlaceholders;

    if(opCode != OperationType.INSERT_CODE && primaryKeys.isEmpty()){
      LOG.error("Primary key columns are missing in records: {}", primaryKeys);
      throw new OnRecordErrorException(record, JdbcErrors.JDBC_62, tableName);
    }

    if (!caseSensitive) {
      switch (opCode) {
        case OperationType.INSERT_CODE:
          valuePlaceholder = String.format("(%s)", joiner.join(columns.values()));
          valuePlaceholders = org.apache.commons.lang3.StringUtils.repeat(valuePlaceholder, ", ", numRecords);
          query = String.format(
              "INSERT INTO %s (%s) VALUES %s",
              tableName,
              joiner.join(columns.keySet()),
              valuePlaceholders
          );
          break;
        case OperationType.DELETE_CODE:
          valuePlaceholder = String.format("(%s)", joiner.join(primaryKeyParams));
          valuePlaceholders = org.apache.commons.lang3.StringUtils.repeat(valuePlaceholder, ", ", numRecords);
          if (multiRow) {
            query = String.format(
                "DELETE FROM %s WHERE (%s) IN (%s)",
                tableName,
                joiner.join(primaryKeys),
                valuePlaceholders
            );
          } else {
            query = String.format(
                "DELETE FROM %s WHERE %s = ?",
                tableName,
                joinerWhereClause.join(primaryKeys)
            );
          }
          break;
        case OperationType.UPDATE_CODE:
          query = String.format(
              "UPDATE %s SET %s = ? WHERE %s = ?",
              tableName,
              joinerColumn.join(columns.keySet()),
              joinerWhereClause.join(primaryKeys)
          );
          break;
        default:
          // Should be checked earlier. Shouldn't reach here
          LOG.error("Unsupported Operation code: {}}", opCode);
          throw new OnRecordErrorException(record, JdbcErrors.JDBC_70, opCode);
      }
    } else {
      switch (opCode) {
        case OperationType.INSERT_CODE:
          valuePlaceholder = String.format("(%s)", joiner.join(columns.values()));
          valuePlaceholders = org.apache.commons.lang3.StringUtils.repeat(valuePlaceholder, ", ", numRecords);
          query = String.format(
              "INSERT INTO %s (\"%s\") VALUES %s",
              tableName,
              joinerWithQuote.join(columns.keySet()),
              valuePlaceholders
          );
          break;
        case OperationType.DELETE_CODE:
          valuePlaceholder = String.format("(%s)", joiner.join(primaryKeyParams));
          valuePlaceholders = org.apache.commons.lang3.StringUtils.repeat(valuePlaceholder, ", ", numRecords);
          if (multiRow) {
            query = String.format(
                "DELETE FROM %s WHERE (\"%s\") IN (%s)",
                tableName,
                joinerWithQuote.join(primaryKeys),
                valuePlaceholders
            );
          } else {
            query = String.format(
                "DELETE FROM %s WHERE \"%s\" = ?",
                tableName,
                joinerWhereClauseWitheQuote.join(primaryKeys)
            );
          }
          break;
        case OperationType.UPDATE_CODE:
          query = String.format(
              "UPDATE %s SET \"%s\" = ? WHERE \"%s\" = ?",
              tableName,
              joinerColumnWithQuote.join(columns.keySet()),
              joinerWhereClauseWitheQuote.join(primaryKeys)
          );
          break;
        default:
          // Should be checked earlier. Shouldn't reach here
          LOG.error("Unsupported Operation code: {}}", opCode);
          throw new OnRecordErrorException(record, JdbcErrors.JDBC_70, opCode);
      }
    }
    return query;
  }

  public PreparedStatement getPreparedStatement(
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      String query,
      Connection connection
  ) throws SQLException {
    PreparedStatement statement;
    if (generatedColumnMappings != null) {
      String[] generatedColumns = new String[generatedColumnMappings.size()];
      for (int i = 0; i < generatedColumnMappings.size(); i++) {
        generatedColumns[i] = generatedColumnMappings.get(i).columnName;
      }
      statement = connection.prepareStatement(query, generatedColumns);
    } else {
      statement = connection.prepareStatement(query);
    }
    return statement;
  }

  public String logError(SQLException e) {
    String formattedError = formatSqlException(e);
    LOG.error(formattedError, e);
    return formattedError;
  }

  /**
   * Generates the no-more-data event
   */
  public void generateNoMoreDataEvent(PushSource.Context context) {
    LOG.info("No More data to process, Triggered No More Data Event");
    BatchContext batchContext = context.startBatch();
    NoMoreDataEvent.EVENT_CREATOR.create(context, batchContext).createAndSend();
    context.processBatch(batchContext);
  }

  /**
   * @return true if the value is an EL string
   */
  public boolean isElString(String value) {
    return value != null && value.startsWith("${");
  }

  /**
   * @return true if the value is a not null string with no EL embedded
   */
  public boolean isPlainString(String value) {
    return value != null && !value.contains(EL_PREFIX);
  }

  public void logDatabaseAndDriverInfo(ConnectionManager connectionManager) throws SQLException {
    DatabaseMetaData databaseMetaData = connectionManager.getConnection().getMetaData();
    LOG.info("Database Product name: {}", databaseMetaData.getDatabaseProductName());
    LOG.info("Database product version: {}", databaseMetaData.getDatabaseProductVersion());
    LOG.info("Driver name: {}", databaseMetaData.getDriverName());
    LOG.info("Driver version: {}", databaseMetaData.getDriverVersion());
  }
}
