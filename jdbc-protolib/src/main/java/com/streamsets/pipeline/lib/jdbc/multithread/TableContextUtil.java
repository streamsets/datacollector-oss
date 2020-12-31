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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.CTTableConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.Groups;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.CDCTableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBeanImpl;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcELEvalContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class TableContextUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TableContext.class);
  //JDBC Result set constants
  private static final String TABLE_METADATA_TABLE_CATALOG_CONSTANT = "TABLE_CAT";
  private static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "TABLE_SCHEM";
  private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "TABLE_NAME";
  private static final String COLUMN_METADATA_COLUMN_NAME = "COLUMN_NAME";
  private static final String COLUMN_METADATA_COLUMN_TYPE = "DATA_TYPE";
  private static final Joiner COMMA_JOINER = Joiner.on(",");
  public static final String GENERIC_PARTITION_SIZE_GT_ZERO_MSG = "partition size must be greater than zero";
  public static final String SQL_SERVER_CDC_TABLE_SUFFIX = "_CT";

  public static final int TYPE_ORACLE_BINARY_FLOAT = 100;
  public static final int TYPE_ORACLE_BINARY_DOUBLE = 101;
  public static final int TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE = -101;
  public static final int TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = -102;
  public static final int TYPE_SQL_SERVER_DATETIMEOFFSET = -155;

  public static final String OFFSET_VALUE_NANO_SEPARATOR = "<n>";
  public static final String TIMESTAMP_NANOS_PATTERN = "^[0-9]+" + OFFSET_VALUE_NANO_SEPARATOR + "[0-9]+$";

  public static final Set<Integer> PARTITIONABLE_TYPES = ImmutableSet.<Integer>builder()
    .add(Types.TINYINT)
    .add(Types.SMALLINT)
    .add(Types.INTEGER)
    .add(Types.BIGINT)
    .add(Types.DATE)
    .add(Types.TIME)
    .add(Types.TIMESTAMP)
    .add(Types.FLOAT)
    .add(Types.REAL)
    .add(Types.DOUBLE)
    .add(Types.DECIMAL)
    .add(Types.NUMERIC)
    .build();

  public static final Map<DatabaseVendor, Set<Integer>> VENDOR_PARTITIONABLE_TYPES = ImmutableMap.<DatabaseVendor, Set<Integer>> builder()
    .put(DatabaseVendor.ORACLE, ImmutableSet.of(
      TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE,
      TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE
    ))
    .put(DatabaseVendor.SQL_SERVER, ImmutableSet.of(
      TYPE_SQL_SERVER_DATETIMEOFFSET
    ))
    .build();

  protected JdbcUtil jdbcUtil;

  public TableContextUtil() {
    this(UtilsProvider.getJdbcUtil());
  }

  public TableContextUtil(JdbcUtil jdbcUtil) {
    this.jdbcUtil = jdbcUtil;
  }

  public JdbcUtil getJdbcUtil() {
    return jdbcUtil;
  }

  protected Map<String, Integer> getColumnNameType(Connection connection, String schema, String tableName) throws SQLException {
    Map<String, Integer> columnNameToType = new LinkedHashMap<>();
    try (ResultSet rs = jdbcUtil.getColumnMetadata(connection, schema, tableName)) {
      while (rs.next()) {
        String column = rs.getString(COLUMN_METADATA_COLUMN_NAME);
        int type = rs.getInt(COLUMN_METADATA_COLUMN_TYPE);
        columnNameToType.put(column, type);
      }
    }
    return columnNameToType;
  }

  /**
   * Our own implementation of JDBCType.valueOf() that won't throw an exception in case of unknown type.
   *
   * @param jdbcType
   * @return
   */
  public static String nameForType(DatabaseVendor vendor, int jdbcType) {
    for( JDBCType sqlType : JDBCType.class.getEnumConstants()) {
      if(jdbcType == sqlType.getVendorTypeNumber())
        return sqlType.name();
    }

    switch (vendor) {
      case ORACLE:
        switch (jdbcType) {
          case TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE: return "TIMESTAMP WITH TIME ZONE";
          case TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE: return "TIMESTAMP WITH LOCAL TIME ZONE";
        }
        break;
      case SQL_SERVER:
        switch (jdbcType) {
          case TYPE_SQL_SERVER_DATETIMEOFFSET: return "SQLSERVER DATETIMEOFFSET";
        }
        break;
    }

    return "Unknown";
  }

  private void checkForUnsupportedOffsetColumns(
      LinkedHashMap<String, Integer> offsetColumnToType
  ) throws StageException {
    //Validate if there are partition column types for offset maintenance
    List<String> unsupportedOffsetColumnAndType = new ArrayList<>();
    for (Map.Entry<String, Integer> offsetColumnToTypeEntry : offsetColumnToType.entrySet()) {
      if (OffsetQueryUtil.UNSUPPORTED_OFFSET_SQL_TYPES.contains(offsetColumnToTypeEntry.getValue())) {
        unsupportedOffsetColumnAndType.add(offsetColumnToTypeEntry.getKey() + " - " + offsetColumnToTypeEntry.getValue());
      }
    }
    if (!unsupportedOffsetColumnAndType.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_69, COMMA_JOINER.join(unsupportedOffsetColumnAndType));
    }
  }

  /**
   * Returns qualified table name (schema.table name)
   * @param schema schema name, can be null
   * @param tableName table name
   * @return qualified table name if schema is not null and tableName alone if schema is null.
   */
  public static String getQualifiedTableName(String schema, String tableName) {
    return StringUtils.isEmpty(schema) ? tableName : schema + "." + tableName ;
  }

  /**
   * Returns quoted qualified table name (schema.table name) based on quoteChar
   * @param schema schema name, can be null
   * @param tableName table name
   * @param quoteCharacter quote character to be attached before and after table and schema name.
   * @return qualified table name if schema is not null and tableName alone if schema is null.
   */
  public static String getQuotedQualifiedTableName(String schema, String tableName, String quoteCharacter) {
    String quoteCharacterLeft = quoteCharacter;
    String quoteCharacterRight = quoteCharacter.equals("[") ? "]" : quoteCharacter;
    String quotedTableName = getQuotedObjectName(tableName, quoteCharacterLeft, quoteCharacterRight);
    return StringUtils.isEmpty(schema) ? quotedTableName : getQuotedObjectName(schema, quoteCharacterLeft, quoteCharacterRight) + "." + quotedTableName;
  }

  /**
   * Quote given object name (column, table name)
   */
  public static String getQuotedObjectName(String objectName, String qLeft, String qRight) {
    return String.format(OffsetQueryUtil.QUOTED_NAME, qLeft, objectName, qRight);
  }

  private TableContext createTableContext(
      DatabaseVendor vendor,
      PushSource.Context context,
      List<Stage.ConfigIssue> issues,
      Connection connection,
      String schemaName,
      String tableName,
      TableConfigBean tableConfigBean,
      TableJdbcELEvalContext tableJdbcELEvalContext,
      QuoteChar quoteChar
  ) throws SQLException, StageException {
    LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
    //Even though we are using this only find partition column's type, we could cache it if need arises.
    final String qualifiedTableName = getQualifiedTableName(schemaName, tableName);
    Map<String, Integer> columnNameToType = getColumnNameType(connection, schemaName, tableName);
    Map<String, String> offsetColumnToStartOffset = new HashMap<>();

    if (tableConfigBean.isOverrideDefaultOffsetColumns()) {
      if (tableConfigBean.getOffsetColumns().isEmpty()) {
        issues.add(context.createConfigIssue(
            Groups.TABLE.name(),
            TableJdbcConfigBean.TABLE_CONFIG,
            JdbcErrors.JDBC_62,
            tableName
        ));
        return null;
      }
      for (String overridenPartitionColumn : tableConfigBean.getOffsetColumns()) {
        if (!columnNameToType.containsKey(overridenPartitionColumn)) {
          issues.add(context.createConfigIssue(
              Groups.TABLE.name(),
              TableJdbcConfigBean.TABLE_CONFIG,
              JdbcErrors.JDBC_63,
              tableName,
              overridenPartitionColumn
          ));
          return null;
        }
        offsetColumnToType.put(overridenPartitionColumn, columnNameToType.get(overridenPartitionColumn));
      }
    } else {
      List<String> primaryKeys = jdbcUtil.getPrimaryKeys(connection, schemaName, tableName);
      if (primaryKeys.isEmpty() && !tableConfigBean.isEnableNonIncremental()) {
        issues.add(context.createConfigIssue(
            Groups.TABLE.name(),
            TableJdbcConfigBean.TABLE_CONFIG,
            JdbcErrors.JDBC_62,
            tableName
        ));
        return null;
      }
      primaryKeys.forEach(primaryKey -> offsetColumnToType.put(primaryKey, columnNameToType.get(primaryKey)));
    }

    checkForUnsupportedOffsetColumns(offsetColumnToType);

    final Map<String, String> offsetColumnMinValues = new HashMap<>();
    final Map<String, String> offsetColumnMaxValues = new HashMap<>();
    if (tableConfigBean.getPartitioningMode() != PartitioningMode.DISABLED) {
      offsetColumnMinValues.putAll(jdbcUtil.getMinimumOffsetValues(
          vendor,
          connection,
          schemaName,
          tableName,
          quoteChar,
          offsetColumnToType.keySet()
      ));
      offsetColumnMaxValues.putAll(jdbcUtil.getMaximumOffsetValues(
          vendor,
          connection,
          schemaName,
          tableName,
          quoteChar,
          offsetColumnToType.keySet()
      ));
    }

    //Initial offset should exist for all partition columns or none at all.
    if (!tableConfigBean.getOffsetColumnToInitialOffsetValue().isEmpty()) {
      Set<String> missingColumns =
          Sets.difference(offsetColumnToType.keySet(), tableConfigBean.getOffsetColumnToInitialOffsetValue().keySet());
      Set<String> extraColumns =
          Sets.difference(tableConfigBean.getOffsetColumnToInitialOffsetValue().keySet(), offsetColumnToType.keySet());

      if (!missingColumns.isEmpty() || !extraColumns.isEmpty()) {
        issues.add(context.createConfigIssue(
            Groups.TABLE.name(),
            TableJdbcConfigBean.TABLE_CONFIG,
            JdbcErrors.JDBC_64,
            missingColumns.isEmpty() ? "(none)" : COMMA_JOINER.join(missingColumns),
            extraColumns.isEmpty() ? "(none)" : COMMA_JOINER.join(extraColumns)
        ));
        return null;
      }

      // Read configured Initial Offset values from config and populate TableContext.offsetColumnToStartOffset
      populateInitialOffsetfromConfig(
          context,
          issues, tableConfigBean.getOffsetColumnToInitialOffsetValue(),
          tableJdbcELEvalContext,
          offsetColumnToStartOffset
      );
      checkForInvalidInitialOffsetValues(
          context,
          issues,
          qualifiedTableName,
          offsetColumnToType,
          offsetColumnToStartOffset
      );
    }

    final Map<String, String> offsetAdjustments = new HashMap<>();
    offsetColumnToType.keySet().forEach(c -> offsetAdjustments.put(c, tableConfigBean.getPartitionSize()));

    return new TableContext(
        vendor,
        quoteChar,
        schemaName,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetAdjustments,
        offsetColumnMinValues,
        offsetColumnMaxValues,
        tableConfigBean.isEnableNonIncremental(),
        tableConfigBean.getPartitioningMode(),
        tableConfigBean.getMaxNumActivePartitions(),
        tableConfigBean.getExtraOffsetColumnConditions()
    );
  }

  /**
   * Evaluate ELs in Initial offsets as needed and populate the final String representation of initial offsets
   * in {@param offsetColumnToStartOffset}
   */
  private void populateInitialOffsetfromConfig(
      PushSource.Context context,
      List<Stage.ConfigIssue> issues,
      Map<String, String> configuredColumnToInitialOffset,
      TableJdbcELEvalContext tableJdbcELEvalContext,
      Map<String, String> offsetColumnToStartOffset
  ) throws StageException {
    for (Map.Entry<String, String> partitionColumnInitialOffsetEntry : configuredColumnToInitialOffset.entrySet()) {
      String value;
      try {
        value = tableJdbcELEvalContext.evaluateAsString(
            "offsetColumnToInitialOffsetValue",
            partitionColumnInitialOffsetEntry.getValue()
        );
        if (value == null) {
          issues.add(context.createConfigIssue(
              Groups.TABLE.name(),
              TableJdbcConfigBean.TABLE_CONFIG,
              JdbcErrors.JDBC_73,
              partitionColumnInitialOffsetEntry.getValue(),
              Utils.format("Expression returned date as null. Check Expression")
          ));
          return;
        }
      } catch (ELEvalException e) {
        issues.add(context.createConfigIssue(
            Groups.TABLE.name(),
            TableJdbcConfigBean.TABLE_CONFIG,
            JdbcErrors.JDBC_73,
            partitionColumnInitialOffsetEntry.getValue(),
            e
        ));
        return;
      }
      offsetColumnToStartOffset.put(
          partitionColumnInitialOffsetEntry.getKey(),
          value
      );
    }
  }

  /**
   * Determines if there are invalid values specified in the initial offset value
   * for columns.
   */
  //@VisibleForTesting
  void checkForInvalidInitialOffsetValues(
      PushSource.Context context,
      List<Stage.ConfigIssue> issues,
      String qualifiedTableName,
      LinkedHashMap<String, Integer> offsetColumnToType,
      Map<String, String> offsetColumnToStartOffset
  ) throws StageException {
    List<String> invalidInitialOffsetFieldAndValue =  new ArrayList<>();
    offsetColumnToType.forEach((offsetColumn, offsetSqlType) -> {
      String initialOffsetValue = offsetColumnToStartOffset.get(offsetColumn);
      try {
        if (jdbcUtil.isSqlTypeOneOf(offsetSqlType, Types.DATE, Types.TIME, Types.TIMESTAMP)) {
          if (jdbcUtil.isSqlTypeOneOf(offsetSqlType, Types.TIMESTAMP)) {
            if (!isTimestampWithNanosFormat(initialOffsetValue)) {
              Long.valueOf(initialOffsetValue);
            }
          } else {
            Long.valueOf(initialOffsetValue);
          }

        } else {
          //Use native field conversion strategy to conver string to specify type and get value
          Field.create(OffsetQueryUtil.SQL_TYPE_TO_FIELD_TYPE.get(offsetSqlType), initialOffsetValue).getValue();
        }
      } catch (IllegalArgumentException e) {
        LOG.error(
            Utils.format(
                "Invalid Initial Offset Value {} for column {} in table {}",
                initialOffsetValue,
                offsetColumn,
                qualifiedTableName
            ),
            e
        );
        invalidInitialOffsetFieldAndValue.add(offsetColumn + " - " + initialOffsetValue);
      }
    });
    if (!invalidInitialOffsetFieldAndValue.isEmpty()) {
      throw new StageException(
          JdbcErrors.JDBC_72,
          qualifiedTableName,
          COMMA_JOINER.join(invalidInitialOffsetFieldAndValue)
      );
    }
  }

  /**
   * Lists all tables matching the {@link TableConfigBeanImpl} and creates a table context for each table.
   * @param connection JDBC connection
   * @param tableConfigBean {@link TableConfigBeanImpl}
   * @return Map of qualified table name to Table Context
   * @throws SQLException If list tables call fails
   * @throws StageException if partition configuration is not correct.
   */
  public Map<String, TableContext> listTablesForConfig(
      DatabaseVendor vendor,
      PushSource.Context context,
      List<Stage.ConfigIssue> issues,
      Connection connection,
      TableConfigBean tableConfigBean,
      TableJdbcELEvalContext tableJdbcELEvalContext,
      QuoteChar quoteChar
  ) throws SQLException, StageException {
    Map<String, TableContext> tableContextMap = new LinkedHashMap<>();
    Pattern tableExclusion =
        StringUtils.isEmpty(tableConfigBean.getTableExclusionPattern()) ?
            null : Pattern.compile(tableConfigBean.getTableExclusionPattern());
    Pattern schemaExclusion =
        StringUtils.isEmpty(tableConfigBean.getSchemaExclusionPattern()) ?
            null : Pattern.compile(tableConfigBean.getSchemaExclusionPattern());
    List<String> tablePatternList =
        tableConfigBean.isTablePatternListProvided() ?
            tableConfigBean.getTablePatternList() : Collections.singletonList(tableConfigBean.getTablePattern());

    // Iterate over all the table Patterns provided and create a LinkedHashMap of type <tableName, TableContext>
    for (String tablePattern : tablePatternList) {
      try (ResultSet rs = jdbcUtil.getTableAndViewMetadata(connection,
          tableConfigBean.getSchema(),
          tablePattern
      )) {
        while (rs.next()) {
          String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
          String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
          if (
              (tableExclusion == null || !tableExclusion.matcher(tableName).matches()) &&
                  (schemaExclusion == null || !schemaExclusion.matcher(schemaName).matches())
          ) {
            TableContext tableContext = createTableContext(
                vendor,
                context,
                issues,
                connection,
                schemaName,
                tableName,
                tableConfigBean,
                tableJdbcELEvalContext,
                quoteChar
            );
            if (tableContext != null) {
              tableContextMap.put(
                  getQualifiedTableName(schemaName, tableName),
                  tableContext
              );
            }
          }
        }
      }
    }
    return tableContextMap;
  }

  public static String getPartitionSizeValidationError(
      DatabaseVendor vendor,
      int colType,
      String column,
      String partitionSize
  ) {
    switch (colType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        try {
          int intVal = Integer.parseInt(partitionSize);
          if (intVal <= 0) {
            return createPartitionSizeValidationError(
                vendor,
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(vendor, column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.BIGINT:
        // TIME, DATE, and TIMESTAMP are represented as long (epoch)
      case Types.TIME:
      case Types.DATE:
      case Types.TIMESTAMP:
        try {
          long longVal = Long.parseLong(partitionSize);
          if (longVal <= 0) {
            return createPartitionSizeValidationError(
                vendor,
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(vendor, column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.FLOAT:
      case Types.REAL:
        try {
          float floatVal = Float.parseFloat(partitionSize);
          if (floatVal <= 0) {
            return createPartitionSizeValidationError(
                vendor,
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(vendor, column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.DOUBLE:
        try {
          double doubleVal = Double.parseDouble(partitionSize);
          if (doubleVal <= 0) {
            return createPartitionSizeValidationError(
                vendor,
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(vendor, column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        try {
          BigDecimal decimalValue = new BigDecimal(partitionSize);
          if (decimalValue.signum() < 1) {
            return createPartitionSizeValidationError(
                vendor,
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(vendor, column, partitionSize, colType, e.getMessage());
        }
        break;
    }
    return null;
  }

  private static String createPartitionSizeValidationError(
      DatabaseVendor vendor,
      String colName,
      String partitionSize,
      int sqlType,
      String errorMsg
  ) {
    return String.format(
        "Partition size of %s is invalid for offset column %s (type %s): %s",
        partitionSize,
        colName,
        nameForType(vendor, sqlType),
        errorMsg
    );
  }

  /**
   * Checks whether the current offset column values are past all max offset values for the table
   *
   * @param tableContext the {@link TableContext} instance that will be checked
   * @param currentColumnOffsets a map from offset columns to current offsets
   * @return true if the current offset value is greater than the max value, for each offset column that has
   *         a max value defined, false otherwise
   */
  public static boolean allOffsetsBeyondMaxValues(
      TableContext tableContext,
      Map<String, String> currentColumnOffsets
  ) {
    return allOffsetsBeyondMaxValues(
        tableContext.getVendor(),
        tableContext.getOffsetColumnToType(),
        currentColumnOffsets,
        tableContext.getOffsetColumnToMaxValues()
    );
  }

  /**
   * Checks whether the current offset column values are past all max offset values for the table
   *
   * @param databaseVendor the database vendor
   * @param offsetColumnToJdbcType a map from offset column name to JDBC types
   * @param currentColumnOffsets a map from offset columns to current offsets
   * @param maxColumnOffsets a map from offset columns to max offsets
   * @return true if the current offset value is greater than the max value, for each offset column that has
   *         a max value defined, false otherwise
   */
  public static boolean allOffsetsBeyondMaxValues(
      DatabaseVendor databaseVendor,
      Map<String, Integer> offsetColumnToJdbcType,
      Map<String, String> currentColumnOffsets,
      Map<String, String> maxColumnOffsets
  ) {
    if (maxColumnOffsets != null) {
      for (Map.Entry<String, String> entry : maxColumnOffsets.entrySet()) {
        final String col = entry.getKey();
        final String max = entry.getValue();
        final int offsetJdbcType = offsetColumnToJdbcType.get(col);
        final String current = currentColumnOffsets.get(col);
        if (current == null || compareOffsetValues(
            offsetJdbcType,
            databaseVendor,
            current,
            max
        ) <= 0) {
          LOG.trace(
              "Current offset {} is less than max offset {} for column {}; returning false from" +
                  " allOffsetsBeyondMaxValues",
              current,
              max,
              col
          );
          return false;
        }
      }
      return true;
    }
    LOG.trace("maxColumnOffsets is null; returning false from allOffsetsBeyondMaxValues");
    return false;
  }

  /**
   * Returns -1, 0, or 1 if the leftOffsetValue is less than, equal to, or greater than, the rightOffsetValue
   * (respectively).
   *
   * @param tableContext the {@link TableContext} instance to look up offset column data from
   * @param column the offset column to check
   * @param leftOffsetValue the left offset value
   * @param rightOffsetValue the right offset value
   * @return -1, 0, or 1 if left &lt; right, left == right, or left &gt; right
   */
  public static int compareOffsetValues(
      TableContext tableContext,
      String column,
      String leftOffsetValue,
      String rightOffsetValue
  ) {
    return compareOffsetValues(
        tableContext.getOffsetColumnToType().get(column),
        tableContext.getVendor(),
        leftOffsetValue,
        rightOffsetValue
    );
  }

  /**
   * Returns -1, 0, or 1 if the leftOffsetValue is less than, equal to, or greater than, the rightOffsetValue
   * (respectively).
   *
   * @param offsetJdbcType the {@link Types} of the offset values
   * @param databaseVendor the database vendor for this database
   * @param leftOffset the left offset value
   * @param rightOffset the right offset value
   * @return -1, 0, or 1 if left &lt; right, left == right, or left &gt; right
   */
  public static int compareOffsetValues(
      int offsetJdbcType,
      DatabaseVendor databaseVendor,
      String leftOffset,
      String rightOffset
  ) {
    if (databaseVendor != null) {
      switch (databaseVendor) {
        case ORACLE:
          if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.ORACLE).contains(offsetJdbcType)) {
            switch (offsetJdbcType) {
              case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE:
                final ZonedDateTime left = ZonedDateTime.parse(leftOffset, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                final ZonedDateTime right = ZonedDateTime.parse(rightOffset, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                return left.compareTo(right);
              default:
                throw new IllegalStateException(Utils.format(
                    "Unsupported type: {} for vendor {}",
                    offsetJdbcType,
                    databaseVendor.name()
                ));
            }
          }
          break;
        case SQL_SERVER:
          if (TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.SQL_SERVER).contains(offsetJdbcType)) {
            if (offsetJdbcType == TableContextUtil.TYPE_SQL_SERVER_DATETIMEOFFSET) {
              final ZonedDateTime left = ZonedDateTime.parse(leftOffset, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
              final ZonedDateTime right = ZonedDateTime.parse(rightOffset, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
              return left.compareTo(right);
            }
          }
          break;
      }
    }

    switch (offsetJdbcType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        final int leftInt = Integer.parseInt(leftOffset);
        final int rightInt = Integer.parseInt(rightOffset);
        return Integer.compare(leftInt, rightInt);
      case Types.TIMESTAMP:
        final Timestamp leftTs = getTimestampForOffsetValue(leftOffset);
        final Timestamp rightTs = getTimestampForOffsetValue(rightOffset);
        return leftTs.compareTo(rightTs);
      case Types.BIGINT:
        // TIME, DATE are represented as long (epoch)
      case Types.TIME:
      case Types.DATE:
        final long leftLong = Long.parseLong(leftOffset);
        final long rightLong = Long.parseLong(rightOffset);
        return Long.compare(leftLong, rightLong);
      case Types.FLOAT:
      case Types.REAL:
        final float leftFloat = Float.parseFloat(leftOffset);
        final float rightFloat = Float.parseFloat(rightOffset);
        return Float.compare(leftFloat, rightFloat);
      case Types.DOUBLE:
        final double leftDouble = Double.parseDouble(leftOffset);
        final double rightDouble = Double.parseDouble(rightOffset);
        return Double.compare(leftDouble, rightDouble);
      case Types.NUMERIC:
      case Types.DECIMAL:
        final BigDecimal leftDecimal = new BigDecimal(leftOffset);
        final BigDecimal rightDecimal = new BigDecimal(rightOffset);
        return leftDecimal.compareTo(rightDecimal);
    }

    throw new IllegalStateException(Utils.format("Unsupported type: {}", offsetJdbcType));
  }


  /**
   * Given 2 maps of type <columnName, offsetValue> - one for input and one to compare input with,
   * Compare and update the inputMap with either the minimum or maximum value for each columnType,
   * based on comparisonType
   *
   * @param tableContext the {@link TableContext} instance to look up offset column data from
   * @param inputMap input Offset Map to compare and modify
   * @param compareMap map to compare with
   * @param comparisonType Whether to update input Map with min or max of values between input Map and compared Map
   * @return modified Input Offset Map
   */
  public static void updateOffsetMapwithMinMax(
      TableContext tableContext,
      Map<String, String> inputMap,
      Map<String,String> compareMap,
      OffsetComparisonType comparisonType
  ) {
    inputMap.replaceAll(
      (columnName, inputValue) -> {
        String comparedValue = compareMap.get(columnName);
        int greaterOrNot = TableContextUtil.compareOffsetValues(tableContext,
            columnName,
            inputValue,
            comparedValue);

        if (comparisonType == OffsetComparisonType.MAXIMUM)
          return (greaterOrNot <= 0) ? comparedValue : inputValue;

        return (greaterOrNot <= 0) ? inputValue : comparedValue;
      });
  }

  public static String generateNextPartitionOffset(
      TableContext tableContext,
      String column,
      String offset
  ) {
    final String partitionSize = tableContext.getOffsetColumnToPartitionOffsetAdjustments().get(column);
    final int offsetColumnType = tableContext.getOffsetColumnToType().get(column);

    switch (tableContext.getVendor()) {
      case ORACLE:
        if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.ORACLE).contains(offsetColumnType)) {
          switch (offsetColumnType) {
            case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE:
              return ZonedDateTime.parse(offset, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .plusSeconds(Integer.parseInt(partitionSize))
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            default:
              throw new IllegalStateException(Utils.format("Unsupported type: {}", offsetColumnType));
          }
        }
        break;
      case SQL_SERVER:
        if(TableContextUtil.VENDOR_PARTITIONABLE_TYPES.get(DatabaseVendor.SQL_SERVER).contains(offsetColumnType)) {
          if (offsetColumnType == TableContextUtil.TYPE_SQL_SERVER_DATETIMEOFFSET) {
            return ZonedDateTime.parse(offset, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .plusSeconds(Integer.parseInt(partitionSize))
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
          }
        }
        break;
    }

    switch (offsetColumnType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        final int int1 = Integer.parseInt(offset);
        final int int2 = Integer.parseInt(partitionSize);
        return String.valueOf(int1 + int2);
      case Types.TIMESTAMP:
        final Timestamp timestamp1 = getTimestampForOffsetValue(offset);
        final long timestampAdj = Long.parseLong(partitionSize);
        final Timestamp timestamp2 = Timestamp.from(timestamp1.toInstant().plusMillis(timestampAdj));
        return getOffsetValueForTimestamp(timestamp2);
      case Types.BIGINT:
      // TIME, DATE are represented as long (epoch)
      case Types.TIME:
      case Types.DATE:
        final long long1 = Long.parseLong(offset);
        final long long2 = Long.parseLong(partitionSize);
        return String.valueOf(long1 + long2);
      case Types.FLOAT:
      case Types.REAL:
        final float float1 = Float.parseFloat(offset);
        final float float2 = Float.parseFloat(partitionSize);
        return String.valueOf(float1 + float2);
      case Types.DOUBLE:
        final double double1 = Double.parseDouble(offset);
        final double double2 = Double.parseDouble(partitionSize);
        return String.valueOf(double1 + double2);
      case Types.NUMERIC:
      case Types.DECIMAL:
        final BigDecimal decimal1 = new BigDecimal(offset);
        final BigDecimal decimal2 = new BigDecimal(partitionSize);
        return decimal1.add(decimal2).toString();
    }
    return null;
  }

  public static String getOffsetValueForTimestamp(Timestamp timestamp) {
    return getOffsetValueForTimestampParts(timestamp.getTime(), timestamp.getNanos());
  }

  public static String getOffsetValueForTimestampParts(long millis, int nanos) {
    // keep only nanos beyond the millisecond precision
    final int nanosAdjusted = nanos % JdbcUtil.NANOS_TO_MILLIS_ADJUSTMENT;
    String nanosStr = nanosAdjusted > 0 ? String.valueOf(nanosAdjusted) : "";
    return String.format("%d%s%s", millis, OFFSET_VALUE_NANO_SEPARATOR, nanosStr);
  }

  /**
   * Checks if timestamp string format is {@code epochMillis<n>nanoseconds}.
   *
   * @param offsetValue The timestamp string
   * @return true if {@param offsetValue} format is {@code epochMillis<n>nanoseconds}, false otherwise.
   */
  public static boolean isTimestampWithNanosFormat(String offsetValue) {
    Pattern timestampNanosStrPattern = Pattern.compile(TIMESTAMP_NANOS_PATTERN);
    return timestampNanosStrPattern.matcher(offsetValue).matches();
  }

  public static Timestamp getTimestampForOffsetValue(String offsetValue) {
    final String[] parts = StringUtils.splitByWholeSeparator(offsetValue, OFFSET_VALUE_NANO_SEPARATOR);
    Utils.checkState(parts.length <= 2, String.format(
        "offsetValue of %s invalid; should contain at most one occurence of nanos separator %s",
        offsetValue,
        OFFSET_VALUE_NANO_SEPARATOR
    ));

    final long millis = Long.parseLong(parts[0]);
    Timestamp timestamp = new Timestamp(millis);

    if (parts.length == 2) {
      final String nanosStr = parts[1];
      if (StringUtils.isNotBlank(nanosStr) && StringUtils.isNumeric(nanosStr)) {
        final int nanos = Integer.parseInt(nanosStr) % JdbcUtil.NANOS_TO_MILLIS_ADJUSTMENT;
        // in a Timestamp, nanos also includes the millisecond portion, so we need to incorporate that
        final long nanosFromMillis = millis % 1000;
        timestamp.setNanos(((int)nanosFromMillis * JdbcUtil.NANOS_TO_MILLIS_ADJUSTMENT) + nanos);
      }
    }

    return timestamp;
  }

  public Map<String, TableContext> listCTTablesForConfig(
      Connection connection,
      CTTableConfigBean tableConfigBean
  ) throws SQLException, StageException {
    Map<String, TableContext> tableContextMap = new LinkedHashMap<>();
    Pattern p =
        StringUtils.isEmpty(tableConfigBean.tableExclusionPattern)?
            null : Pattern.compile(tableConfigBean.tableExclusionPattern);

    long currentVersion = getCurrentVersion(connection);

    try (ResultSet rs = jdbcUtil.getTableMetadata(connection, tableConfigBean.schema, tableConfigBean.tablePattern)) {
      while (rs.next()) {
        String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
        String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
        if (p == null || !p.matcher(tableName).matches()) {
          // validate table is change tracking enabled
          try {
            long min_valid_version = validateTable(connection, schemaName, tableName);
            if (min_valid_version <= currentVersion) {
              tableContextMap.put(
                  getQualifiedTableName(schemaName, tableName),
                  createCTTableContext(connection, schemaName, tableName, tableConfigBean, currentVersion)
              );
            } else {
              LOG.debug(JdbcErrors.JDBC_200.getMessage(), schemaName + "." + tableName);
            }
          } catch (SQLException | StageException e) {
            LOG.debug(JdbcErrors.JDBC_200.getMessage(), schemaName + "." + tableName);
          }
        }
      }
    }

    return tableContextMap;
  }

  public Map<String, TableContext> listCDCTablesForConfig(
      Connection connection,
      CDCTableConfigBean tableConfigBean,
      boolean enableSchemaChanges
  ) throws SQLException, StageException {
    Map<String, TableContext> tableContextMap = new LinkedHashMap<>();
    Pattern p =
        StringUtils.isEmpty(tableConfigBean.tableExclusionPattern)?
            null : Pattern.compile(tableConfigBean.tableExclusionPattern);

    final String tablePattern = tableConfigBean.capture_instance + SQL_SERVER_CDC_TABLE_SUFFIX;
    final String cdcSchema = "cdc";
    try (ResultSet rs = jdbcUtil.getTableMetadata(connection, cdcSchema, tablePattern)) {
      while (rs.next()) {
        String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
        String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);

        if (p == null || !p.matcher(tableName).matches()) {
          // get the all column infos
          Map<String, Integer> columnNameToType = null;
          if (enableSchemaChanges) {
            columnNameToType = getColumnNameType(connection, schemaName, tableName);
            // remove CDC specific columns
            columnNameToType.remove(MSQueryUtil.CDC_START_LSN);
            columnNameToType.remove(MSQueryUtil.CDC_END_LSN);
            columnNameToType.remove(MSQueryUtil.CDC_SEQVAL);
            columnNameToType.remove(MSQueryUtil.CDC_OPERATION);
            columnNameToType.remove(MSQueryUtil.CDC_UPDATE_MASK);
            columnNameToType.remove(MSQueryUtil.CDC_COMMAND_ID);
          }

          try {
              tableContextMap.put(
                  getQualifiedTableName(schemaName, tableName),
                  createCDCTableContext(schemaName, tableName, tableConfigBean.initialOffset, columnNameToType)
              );
          } catch (SQLException | StageException e) {
            LOG.error(JdbcErrors.JDBC_200.getMessage(), schemaName + "." + tableName, e);
          }
        }
      }
    }

    return tableContextMap;
  }

  private long getCurrentVersion(Connection connection) throws SQLException, StageException {
    PreparedStatement preparedStatement = connection.prepareStatement(MSQueryUtil.getCurrentVersion());
    ResultSet resultSet = preparedStatement.executeQuery();
    while (resultSet.next()) {
      return resultSet.getLong(1);
    }

    throw new StageException(JdbcErrors.JDBC_201, -1);
  }

  private long validateTable(Connection connection, String schema, String table) throws SQLException, StageException {
    String query = MSQueryUtil.getMinVersion(schema, table);
    PreparedStatement validation = connection.prepareStatement(query);
    ResultSet resultSet = validation.executeQuery();
    if (resultSet.next()) {
      return resultSet.getLong("min_valid_version");
    }

    throw new StageException(JdbcErrors.JDBC_200, table);
  }

  private TableContext createCTTableContext(
      Connection connection,
      String schemaName,
      String tableName,
      CTTableConfigBean tableConfigBean,
      long currentSyncVersion
  ) throws SQLException, StageException {
    LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
    //Even though we are using this only find partition column's type, we could cache it if need arises.
    Map<String, Integer> columnNameToType = getColumnNameType(connection, schemaName, tableName);
    Map<String, String> offsetColumnToStartOffset = new HashMap<>();

    List<String> primaryKeys = jdbcUtil.getPrimaryKeys(connection, schemaName, tableName);
    if (primaryKeys.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_62, tableName);
    }
    primaryKeys.forEach(primaryKey -> offsetColumnToType.put(primaryKey, columnNameToType.get(primaryKey)));

    offsetColumnToType.put(MSQueryUtil.SYS_CHANGE_VERSION, MSQueryUtil.SYS_CHANGE_VERSION_TYPE);

    long initalSyncVersion = tableConfigBean.initialOffset;

    if (tableConfigBean.initialOffset < 0) {
      initalSyncVersion = currentSyncVersion;
    }

    final Map<String, String> offsetAdjustments = new HashMap<>();
    final Map<String, String> offsetColumnMinValues = new HashMap<>();
    final Map<String, String> offsetColumnMaxValues = new HashMap<>();
    final PartitioningMode partitioningMode = TableConfigBean.PARTITIONING_MODE_DEFAULT_VALUE;
    final int maxNumActivePartitions = 0;
    final String extraOffsetColumnConditions = "";

    return new TableContext(
        DatabaseVendor.SQL_SERVER,
        QuoteChar.NONE,
        schemaName,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetAdjustments,
        offsetColumnMinValues,
        offsetColumnMaxValues,
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
        partitioningMode,
        maxNumActivePartitions,
        extraOffsetColumnConditions,
        initalSyncVersion
    );
  }

  private TableContext createCDCTableContext(
      String schemaName,
      String tableName,
      String initalOffset,
      Map<String, Integer> columnToType
  ) throws SQLException, StageException {
    LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
    Map<String, String> offsetColumnToStartOffset = new HashMap<>();

    if (!Strings.isNullOrEmpty(initalOffset)) {
      offsetColumnToStartOffset.put(MSQueryUtil.CDC_START_LSN, initalOffset);
    }

    offsetColumnToType.put(MSQueryUtil.CDC_START_LSN, Types.VARBINARY);
    offsetColumnToType.put(MSQueryUtil.CDC_SEQVAL, Types.VARBINARY);
    offsetColumnToType.put(MSQueryUtil.CDC_TXN_WINDOW, Types.INTEGER);

    final Map<String, String> offsetAdjustments = new HashMap<>();
    final Map<String, String> offsetColumnMinValues = new HashMap<>();
    final Map<String, String> offsetColumnMaxValues = new HashMap<>();
    final PartitioningMode partitioningMode = TableConfigBean.PARTITIONING_MODE_DEFAULT_VALUE;
    final int maxNumActivePartitions = 0;
    final String extraOffsetColumnConditions = "";

    TableContext tableContext = new TableContext(
        DatabaseVendor.SQL_SERVER,
        QuoteChar.NONE,
        schemaName,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetAdjustments,
        offsetColumnMinValues,
        offsetColumnMaxValues,
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
        partitioningMode,
        maxNumActivePartitions,
        extraOffsetColumnConditions
    );

    if (columnToType != null) {
      tableContext.setColumnToType(columnToType);
    }

    return tableContext;
  }

}
