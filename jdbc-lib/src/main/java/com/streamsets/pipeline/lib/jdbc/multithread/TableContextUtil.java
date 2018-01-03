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
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.CTTableConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.Groups;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.CDCTableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class TableContextUtil {
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

  public static final String OFFSET_VALUE_NANO_SEPARATOR = "<n>";

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


  private TableContextUtil() {}

  private static Map<String, Integer> getColumnNameType(Connection connection, String schema, String tableName) throws SQLException {
    Map<String, Integer> columnNameToType = new LinkedHashMap<>();
    try (ResultSet rs = JdbcUtil.getColumnMetadata(connection, schema, tableName)) {
      while (rs.next()) {
        String column = rs.getString(COLUMN_METADATA_COLUMN_NAME);
        int type = rs.getInt(COLUMN_METADATA_COLUMN_TYPE);
        columnNameToType.put(column, type);
      }
    }
    return columnNameToType;
  }

  private static void checkForUnsupportedOffsetColumns(
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
   * @param qC quote character to be attached before and after table and schema name.
   * @return qualified table name if schema is not null and tableName alone if schema is null.
   */
  public static String getQuotedQualifiedTableName(String schema, String tableName, String qC) {
    String quotedTableName = String.format(OffsetQueryUtil.QUOTED_NAME, qC, tableName, qC);
    return StringUtils.isEmpty(schema) ?
        quotedTableName: String.format(OffsetQueryUtil.QUOTED_NAME, qC, schema, qC)  + "." + quotedTableName ;
  }

  private static TableContext createTableContext(
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

    if (tableConfigBean.overrideDefaultOffsetColumns) {
      if (tableConfigBean.offsetColumns.isEmpty()) {
        issues.add(context.createConfigIssue(
            Groups.TABLE.name(),
            TableJdbcConfigBean.TABLE_CONFIG,
            JdbcErrors.JDBC_62,
            tableName
        ));
        return null;
      }
      for (String overridenPartitionColumn : tableConfigBean.offsetColumns) {
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
      List<String> primaryKeys = JdbcUtil.getPrimaryKeys(connection, schemaName, tableName);
      if (primaryKeys.isEmpty() && !tableConfigBean.enableNonIncremental) {
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

    Map<String, String> offsetColumnMinValues = new HashMap<>();
    if (tableConfigBean.partitioningMode != PartitioningMode.DISABLED) {
      offsetColumnMinValues.putAll(JdbcUtil.getMinimumOffsetValues(
          connection,
          schemaName,
          tableName,
          quoteChar,
          offsetColumnToType.keySet()
      ));
    }

    //Initial offset should exist for all partition columns or none at all.
    if (!tableConfigBean.offsetColumnToInitialOffsetValue.isEmpty()) {
      Set<String> missingColumns =
          Sets.difference(offsetColumnToType.keySet(), tableConfigBean.offsetColumnToInitialOffsetValue.keySet());
      Set<String> extraColumns =
          Sets.difference(tableConfigBean.offsetColumnToInitialOffsetValue.keySet(), offsetColumnToType.keySet());

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

      populateInitialOffset(
          context,
          issues,
          tableConfigBean.offsetColumnToInitialOffsetValue,
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
    offsetColumnToType.keySet().forEach(c -> offsetAdjustments.put(c, tableConfigBean.partitionSize));

    return new TableContext(
        schemaName,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetAdjustments,
        offsetColumnMinValues,
        tableConfigBean.enableNonIncremental,
        tableConfigBean.partitioningMode,
        tableConfigBean.maxNumActivePartitions,
        tableConfigBean.extraOffsetColumnConditions
    );
  }

  /**
   * Evaluate ELs in Initial offsets as needed and populate the final String representation of initial offsets
   * in {@param offsetColumnToStartOffset}
   */
  private static void populateInitialOffset(
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
  static void checkForInvalidInitialOffsetValues(
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
        if (JdbcUtil.isSqlTypeOneOf(offsetSqlType, Types.DATE, Types.TIME, Types.TIMESTAMP)) {
          Long.valueOf(initialOffsetValue); //NOSONAR
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
   * Lists all tables matching the {@link TableConfigBean} and creates a table context for each table.
   * @param connection JDBC connection
   * @param tableConfigBean {@link TableConfigBean}
   * @return Map of qualified table name to Table Context
   * @throws SQLException If list tables call fails
   * @throws StageException if partition configuration is not correct.
   */
  public static Map<String, TableContext> listTablesForConfig(
      PushSource.Context context,
      List<Stage.ConfigIssue> issues,
      Connection connection,
      TableConfigBean tableConfigBean,
      TableJdbcELEvalContext tableJdbcELEvalContext,
      QuoteChar quoteChar
  ) throws SQLException, StageException {
    Map<String, TableContext> tableContextMap = new LinkedHashMap<>();
    Pattern p =
        StringUtils.isEmpty(tableConfigBean.tableExclusionPattern)?
            null : Pattern.compile(tableConfigBean.tableExclusionPattern);
    try (ResultSet rs = JdbcUtil.getTableMetadata(connection, null, tableConfigBean.schema, tableConfigBean.tablePattern, true)) {
      while (rs.next()) {
        String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
        String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
        if (p == null || !p.matcher(tableName).matches()) {
          TableContext tableContext = createTableContext(
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
    return tableContextMap;
  }

  public static String getPartitionSizeValidationError(
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
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(column, partitionSize, colType, e.getMessage());
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
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.FLOAT:
      case Types.REAL:
        try {
          float floatVal = Float.parseFloat(partitionSize);
          if (floatVal <= 0) {
            return createPartitionSizeValidationError(
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.DOUBLE:
        try {
          double doubleVal = Double.parseDouble(partitionSize);
          if (doubleVal <= 0) {
            return createPartitionSizeValidationError(
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(column, partitionSize, colType, e.getMessage());
        }
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        try {
          BigDecimal decimalValue = new BigDecimal(partitionSize);
          if (decimalValue.signum() < 1) {
            return createPartitionSizeValidationError(
                column,
                partitionSize,
                colType,
                GENERIC_PARTITION_SIZE_GT_ZERO_MSG
            );
          }
        } catch (NumberFormatException e) {
          return createPartitionSizeValidationError(column, partitionSize, colType, e.getMessage());
        }
        break;
    }
    return null;
  }

  private static String createPartitionSizeValidationError(
      String colName,
      String partitionSize,
      int sqlType,
      String errorMsg
  ) {
    return String.format(
        "Partition size of %s is invalid for offset column %s (type %s): %s",
        partitionSize,
        colName,
        JDBCType.valueOf(sqlType).getName(),
        errorMsg
    );
  }

  public static String generateNextPartitionOffset(
      TableContext tableContext,
      String column,
      String offset
  ) {
    final String partitionSize = tableContext.getOffsetColumnToPartitionOffsetAdjustments().get(column);
    switch (tableContext.getOffsetColumnToType().get(column)) {
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

  public static Map<String, TableContext> listCTTablesForConfig(
      Connection connection,
      CTTableConfigBean tableConfigBean
  ) throws SQLException, StageException {
    Map<String, TableContext> tableContextMap = new LinkedHashMap<>();
    Pattern p =
        StringUtils.isEmpty(tableConfigBean.tableExclusionPattern)?
            null : Pattern.compile(tableConfigBean.tableExclusionPattern);

    long currentVersion = getCurrentVersion(connection);

    try (ResultSet rs = JdbcUtil.getTableMetadata(connection, null, tableConfigBean.schema, tableConfigBean.tablePattern, false)) {
      while (rs.next()) {
        String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
        String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
        if (p == null || !p.matcher(tableName).matches()) {
          // validate table is change tracking enabled
          try {
            long min_valid_version = validateTable(connection, tableName);
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

  public static Map<String, TableContext> listCDCTablesForConfig(
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
    try (ResultSet rs = JdbcUtil.getTableMetadata(connection, null, cdcSchema, tablePattern, false)) {
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

  private static long getCurrentVersion(Connection connection) throws SQLException, StageException {
    PreparedStatement preparedStatement = connection.prepareStatement(MSQueryUtil.getCurrentVersion());
    ResultSet resultSet = preparedStatement.executeQuery();
    while (resultSet.next()) {
      return resultSet.getLong(1);
    }

    throw new StageException(JdbcErrors.JDBC_201, -1);
  }

  private static long validateTable(Connection connection, String table) throws SQLException, StageException {
    PreparedStatement validation = connection.prepareStatement(String.format(MSQueryUtil.getMinVersion(), table));
    ResultSet resultSet = validation.executeQuery();
    if (resultSet.next()) {
      return resultSet.getLong("min_valid_version");
    }

    throw new StageException(JdbcErrors.JDBC_200, table);
  }

  private static TableContext createCTTableContext(
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

    List<String> primaryKeys = JdbcUtil.getPrimaryKeys(connection, schemaName, tableName);
    if (primaryKeys.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_62, tableName);
    }
    primaryKeys.forEach(primaryKey -> offsetColumnToType.put(primaryKey, columnNameToType.get(primaryKey)));

    offsetColumnToType.put(MSQueryUtil.SYS_CHANGE_VERSION, MSQueryUtil.SYS_CHANGE_VERSION_TYPE);

    long initalSyncVersion = tableConfigBean.initialOffset;

    if (tableConfigBean.initialOffset < 0) {
      initalSyncVersion = currentSyncVersion;
    }
    offsetColumnToStartOffset.put(MSQueryUtil.SYS_CHANGE_VERSION, Long.toString(initalSyncVersion));

    final Map<String, String> offsetAdjustments = new HashMap<>();
    final Map<String, String> offsetColumnMinValues = new HashMap<>();
    final PartitioningMode partitioningMode = TableConfigBean.PARTITIONING_MODE_DEFAULT_VALUE;
    final int maxNumActivePartitions = 0;
    final String extraOffsetColumnConditions = "";

    return new TableContext(
        schemaName,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetAdjustments,
        offsetColumnMinValues,
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
        partitioningMode,
        maxNumActivePartitions,
        extraOffsetColumnConditions
    );
  }

  private static TableContext createCDCTableContext(
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

    final Map<String, String> offsetAdjustments = new HashMap<>();
    final Map<String, String> offsetColumnMinValues = new HashMap<>();
    final PartitioningMode partitioningMode = TableConfigBean.PARTITIONING_MODE_DEFAULT_VALUE;
    final int maxNumActivePartitions = 0;
    final String extraOffsetColumnConditions = "";

    TableContext tableContext = new TableContext(
        schemaName,
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetAdjustments,
        offsetColumnMinValues,
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
