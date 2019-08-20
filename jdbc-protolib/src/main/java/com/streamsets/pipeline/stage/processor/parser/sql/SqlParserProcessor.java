/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode;
import com.streamsets.pipeline.lib.jdbc.PrecisionAndScale;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.parser.sql.DateTimeColumnHandler;
import com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil;
import com.streamsets.pipeline.lib.jdbc.parser.sql.SQLListener;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnparseableSQLException;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.zaxxer.hikari.HikariDataSource;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_402;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_403;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_85;
import static com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil.JDBCTypeNames;
import static com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil.getOperation;

public class SqlParserProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SqlParserProcessor.class);
  public static final String INSERT = "INSERT";
  public static final String UPDATE = "UPDATE";
  public static final String DELETE = "DELETE";
  private static final String UNSUPPORTED_OPERATION = "Unsupported Operation: '{}'";
  private static final String SENDING_TO_ERROR_AS_CONFIGURED = ". Sending to error as configured";
  private static final String UNSUPPORTED_TO_ERR = JDBC_85.getMessage() + SENDING_TO_ERROR_AS_CONFIGURED;
  private static final String DISCARDING_RECORD_AS_CONFIGURED = ". Discarding record as configured";
  private static final String UNSUPPORTED_DISCARD = JDBC_85.getMessage() + DISCARDING_RECORD_AS_CONFIGURED;
  private static final String UNSUPPORTED_SEND_TO_PIPELINE =
      JDBC_85.getMessage() + ". Sending to pipeline as configured";

  @VisibleForTesting
  static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "TABLE_SCHEM";
  @VisibleForTesting
  static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "TABLE_NAME";
  @VisibleForTesting
  static final String TABLE = "sql.table";

  private final SqlParserConfigBean configBean;
  private ErrorRecordHandler errorRecordHandler;
  private final SQLListener listener;
  private HikariDataSource dataSource;

  @VisibleForTesting
  Connection connection;
  private DateTimeColumnHandler dateTimeColumnHandler;
  private final Map<SchemaAndTable, Map<String, String>> dateTimeColumns = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, Integer>> tableSchemas = new HashMap<>();
  private final ParseTreeWalker parseTreeWalker = new ParseTreeWalker();

  private final JdbcUtil jdbcUtil;

  private static final String CONNECTION_STR = "configBean.hikariConfigBean.connectionString";

  SqlParserProcessor(SqlParserConfigBean configBean) {
    this.configBean = configBean;
    this.listener = new SQLListener();
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if (configBean.resolveSchema) {
      issues = configBean.hikariConfigBean.init(getContext(), issues);
      if (issues.isEmpty() && connection == null) {
        try {
          dataSource = jdbcUtil.createDataSourceForRead(configBean.hikariConfigBean.getUnderlying());
          connection = dataSource.getConnection();
          connection.setAutoCommit(false);
        } catch (StageException e) {
          LOG.error("Error while connecting to DB", e);
          issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STR, e.getErrorCode(), e.getParams()));
          return issues;
        } catch ( SQLException e) {
          LOG.error("Error while connecting to DB", e);
          issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STR, JDBC_00, e.toString()));
          return issues;
        }
      }
    }
    dateTimeColumnHandler =
        new DateTimeColumnHandler(
            ZoneId.of(configBean.dbTimeZone),
            false,
            configBean.dateFormat,
            configBean.localDatetimeFormat,
            configBean.zonedDatetimeFormat
        );
    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Iterator<Record> recordIterator = batch.getRecords();
    while (recordIterator.hasNext()) {
      process(recordIterator.next()).ifPresent(singleLaneBatchMaker::addRecord);
    }
  }

  private Optional<Record> process(Record record) throws StageException {
    String sql = null;
    if (record.has(configBean.sqlField)) {
      sql = record.get(configBean.sqlField).getValueAsString();
    }
    if (StringUtils.isEmpty(sql)) {
      errorRecordHandler.onError(new OnRecordErrorException(record, JdbcErrors.JDBC_401, record, configBean.sqlField));
      return Optional.empty();
    }
    int op = getOp(record.get(configBean.sqlField).getValueAsString());
    try {
      ParserRuleContext ruleContext = ParseUtil.getParserRuleContext(sql, op);
      listener.reset();
      parseTreeWalker.walk(listener, ruleContext);
      String schema = listener.getSchema();
      String table = listener.getTable();
      if (StringUtils.isNotEmpty(table)) {
        record.getHeader().setAttribute(TABLE, table);
        if (record.getHeader().getAttribute(TABLE_METADATA_TABLE_NAME_CONSTANT) == null) {
          record.getHeader().setAttribute(TABLE_METADATA_TABLE_NAME_CONSTANT, table);
        }
      }
      if (StringUtils.isNotEmpty(schema)
          && record.getHeader().getAttribute(TABLE_METADATA_TABLE_SCHEMA_CONSTANT) == null) {
        record.getHeader().setAttribute(TABLE_METADATA_TABLE_SCHEMA_CONSTANT, schema);
      }
      Map<String, String> columns = listener.getColumns();
      final SchemaAndTable schemaAndTable = new SchemaAndTable(schema, table);
      if (configBean.resolveSchema) {
        if (!tableSchemas.containsKey(schemaAndTable)) {
          resolveSchema(schemaAndTable);
        }
        boolean isRetry = false;
        while (true) {
          try {
            record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(getOperation(op)));
            if (addFields(schemaAndTable, columns, record)) {
              return Optional.of(record);
            } else {
              return Optional.empty();
            }
          } catch (SchemaMismatchException e) {
            if (isRetry) {
              errorRecordHandler.onError(
                  new OnRecordErrorException(
                      record,
                      JDBC_402,
                      Joiner.on(",").join(e.missingCols),
                      table
                  ));
              return Optional.empty();
            }
            resolveSchema(schemaAndTable);
            isRetry = true;
          }
        }
      } else {
        record.set(configBean.resultFieldPath, createUnparsedField(columns));
        return Optional.of(record);
      }
    } catch (UnparseableSQLException ex) {
      errorRecordHandler.onError(new OnRecordErrorException(record, JDBC_403, sql));
      return Optional.empty(); // Record has been sent to error, so don't return it.
    }
  }

  private boolean handleUnsupportedFieldTypes(Record r, String error) {
    switch (configBean.unsupportedFieldOp) {
      case SEND_TO_PIPELINE:
        LOG.warn(Utils.format(UNSUPPORTED_SEND_TO_PIPELINE, error, r.getHeader().getAttribute(TABLE)));
        return true;
      case TO_ERROR:
        LOG.warn(Utils.format(UNSUPPORTED_TO_ERR, error, r.getHeader().getAttribute(TABLE)));
        getContext().toError(r, JDBC_85, error, r.getHeader().getAttribute(TABLE));
        return false;
      case DISCARD:
        LOG.warn(Utils.format(UNSUPPORTED_DISCARD, error, r.getHeader().getAttribute(TABLE)));
        return false;
      default:
        throw new IllegalStateException("Unknown Record Handling option");
    }
  }

  private boolean addFields(
      SchemaAndTable table,
      Map<String, String> columns,
      Record record
  ) throws SchemaMismatchException, StageException {
    Map<String, Field> fields = new HashMap<>();
    List<String> mismatched = new ArrayList<>();
    List<UnsupportedFieldTypeException> unsupportedFieldTypeExceptions = new LinkedList<>();
    for (Map.Entry<String, String> col : columns.entrySet()) {
      final String columnName = col.getKey();
      Integer type = tableSchemas.get(table).get(columnName);
      if (type == null) {
        mismatched.add(columnName);
      } else {
        try {
          Field f =
              ParseUtil.generateField(columnName, col.getValue(), type, dateTimeColumnHandler);
          fields.put(columnName, f);
          if (decimalColumns.containsKey(table) && decimalColumns.get(table).containsKey(columnName)) {
            int precision = decimalColumns.get(table).get(columnName).precision;
            int scale = decimalColumns.get(table).get(columnName).scale;
            record.getHeader().setAttribute("jdbc." + columnName + ".precision", String.valueOf(precision));
            record.getHeader().setAttribute("jdbc." + columnName + ".scale", String.valueOf(scale));
          }
        } catch (UnsupportedFieldTypeException ufx) {
          unsupportedFieldTypeExceptions.add(ufx);
        }
      }
    }
    if (!mismatched.isEmpty()) {
      throw new SchemaMismatchException(table, mismatched);
    }

    if (configBean.sendUnsupportedFields) {
      unsupportedFieldTypeExceptions.forEach(ex -> fields.put(ex.column, Field.create(ex.columnVal)));
    }
    record.set(configBean.resultFieldPath, Field.create(fields));
    List<String> errorColumns;
    if (!unsupportedFieldTypeExceptions.isEmpty()) {
      errorColumns = unsupportedFieldTypeExceptions.stream().map(ex -> {
            String fieldTypeName =
                JDBCTypeNames.getOrDefault(ex.fieldType, "unknown");
            return "[Column = '" + ex.column + "', Type = '" + fieldTypeName + "', Value = '" + ex.columnVal + "']";
          }
      ).collect(Collectors.toList());
      return handleUnsupportedFieldTypes(record, Joiner.on(",").join(errorColumns));
    }
    return true;
  }


  private void resolveSchema(SchemaAndTable schemaAndTable) throws StageException {
    Map<String, Integer> columns = new HashMap<>();
    String schema = schemaAndTable.getSchema();
    String table = schemaAndTable.getTable();
    try (Statement s = connection.createStatement()) {
      ResultSetMetaData md = s.executeQuery(
          Utils.format(
              "SELECT * FROM {}{} WHERE 1 = 0",
              StringUtils.isNotEmpty(schema) ? "\"" + schema + "\"." : "",
              "\"" + table + "\""
          )).getMetaData();
      int colCount = md.getColumnCount();
      for (int i = 1; i <= colCount; i++) {
        int colType = md.getColumnType(i);
        String colName = md.getColumnName(i);
        if (!configBean.caseSensitive) {
          colName = colName.toUpperCase();
        }
        if (colType == Types.DATE || colType == Types.TIME || colType == Types.TIMESTAMP) {
          dateTimeColumns.computeIfAbsent(schemaAndTable, k -> new HashMap<>());
          dateTimeColumns.get(schemaAndTable).put(colName, md.getColumnTypeName(i));
        }

        if (colType == Types.DECIMAL || colType == Types.NUMERIC) {
          decimalColumns
              .computeIfAbsent(schemaAndTable, k -> new HashMap<>())
              .put(colName, new PrecisionAndScale(md.getPrecision(i), md.getScale(i)));
        }
        columns.put(md.getColumnName(i), md.getColumnType(i));
      }
      tableSchemas.put(schemaAndTable, columns);
    } catch (SQLException ex) {
      throw new StageException(JDBC_00, configBean.hikariConfigBean.connectionString);
    }
  }

  private Field createUnparsedField(Map<String, String> unparsed) {
    Map<String, Field> fields = new HashMap<>();
    unparsed.forEach((k, v) -> fields.put(k, Field.create(v)));
    return Field.create(fields);
  }

  private int getOp(String sql) {
    String operationStr = sql.split("\\s+")[0];
    if (INSERT.equalsIgnoreCase(operationStr)) {
      return OracleCDCOperationCode.INSERT_CODE;
    } else if (UPDATE.equalsIgnoreCase(operationStr)) {
      return OracleCDCOperationCode.UPDATE_CODE;
    } else if (DELETE.equalsIgnoreCase(operationStr)) {
      return OracleCDCOperationCode.DELETE_CODE;
    } else {
      throw new UnsupportedOperationException(Utils.format(UNSUPPORTED_OPERATION, operationStr));
    }
  }

  private class SchemaMismatchException extends Exception {

    final SchemaAndTable table;
    final List<String> missingCols;

    public SchemaMismatchException(SchemaAndTable table, List<String> missingCols) {
      this.table = table;
      this.missingCols = missingCols;
    }
  }
}
