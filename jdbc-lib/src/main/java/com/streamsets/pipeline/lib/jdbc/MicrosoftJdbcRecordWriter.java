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

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.jdbc.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Record writer implementation that resolves a Microsoft change data capture log
 * to the queries required to replicate the table to another destination.
 */
public class MicrosoftJdbcRecordWriter implements JdbcRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MicrosoftJdbcRecordWriter.class);

  private static final int DELETE = 1;
  private static final int INSERT = 2;
  private static final int BEFORE_UPDATE = 3;
  private static final int AFTER_UPDATE = 4;

  public static final ChangeLogFormat FORMAT = ChangeLogFormat.MSSQL;
  public static final String OP_FIELD = "/__$operation";

  private final String tableName;
  private final DataSource dataSource;

  private List<String> primaryKeyColumns;

  public MicrosoftJdbcRecordWriter(
    DataSource dataSource,
    String tableName) throws StageException {
    this.dataSource = dataSource;
    this.tableName = tableName;

    lookupPrimaryKeys();
  }

  private void lookupPrimaryKeys() throws StageException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      primaryKeyColumns = JdbcUtil.getPrimaryKeys(connection, tableName);
    } catch (SQLException e) {
      String formattedError = JdbcUtil.formatSqlException(e);
      LOG.error(formattedError);
      LOG.debug(formattedError, e);
      throw new StageException(Errors.JDBCDEST_17, tableName, formattedError);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          String formattedError = JdbcUtil.formatSqlException(e);
          LOG.error(formattedError);
          LOG.debug(formattedError, e);
        }
      }
    }

    if (primaryKeyColumns.isEmpty()) {
      throw new StageException(Errors.JDBCDEST_17, tableName);
    }

  }

  /** {@inheritDoc} */
  @Override
  public List<OnRecordErrorException> writeBatch(Batch batch) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    try {
      connection = dataSource.getConnection();

      Iterator<Record> recordIterator = batch.getRecords();

      recordLoop:
      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();

        String query;
        int i;
        Map<String, Object> columnMappings = getColumnMappings(record);
        if (record.has(OP_FIELD)) {
          int operation = record.get(OP_FIELD).getValueAsInteger();
          switch (operation) {
            case INSERT:
              query = String.format(
                  "INSERT INTO %s (%s) VALUES (%s)",
                  tableName,
                  Joiner.on(", ").join(columnMappings.keySet()),
                  Joiner.on(", ").join(Collections.nCopies(columnMappings.size(), "?"))
              );
              break;
            case BEFORE_UPDATE:
              continue;
            case AFTER_UPDATE:
              query = String.format(
                  "UPDATE %s SET %s = ? WHERE %s = ?",
                  tableName,
                  Joiner.on(" = ?, ").join(columnMappings.keySet()),
                  Joiner.on(" = ? AND ").join(primaryKeyColumns)
              );
              break;
            case DELETE:
              query = String.format(
                  "DELETE FROM %s WHERE %s = ?",
                  tableName,
                  Joiner.on(" = ? AND ").join(primaryKeyColumns)
              );
              break;
            default:
              errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_09, operation, FORMAT));
              continue;
          }

          LOG.debug("Prepared Query: {}", query);
          PreparedStatement statement = connection.prepareStatement(query);

          i = 1;
          if (operation != DELETE) {
            for (Object value : columnMappings.values()) {
              statement.setObject(i, value);
              ++i;
            }
          }

          if (operation == AFTER_UPDATE || operation == DELETE) {
            // Also bind the primary keys for the where clause
            for (String key : primaryKeyColumns) {
              if (!columnMappings.containsKey(key)) {
                errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_19, key));
                continue recordLoop;
              }
              statement.setObject(i, columnMappings.get(key));
              ++i;
            }
          }

          // Since we must commit all the changes in the same transaction, if one fails,
          // we should abort the entire transaction (batch).
          LOG.debug("Bound Query: {}", statement.toString());
          statement.execute();
          statement.close();
        } else {
          errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_08, OP_FIELD, FORMAT));
        }
      }
      connection.commit();
    } catch (SQLException e) {
      String formattedError = JdbcUtil.formatSqlException(e);
      LOG.error(formattedError);
      LOG.debug(formattedError, e);
      // Whole batch fails
      errorRecords.clear();
      Iterator<Record> records = batch.getRecords();
      while (records.hasNext()) {
        errorRecords.add(new OnRecordErrorException(records.next(), Errors.JDBCDEST_14, formattedError));
      }
    } finally {
      if (connection != null) {
        try {
          connection.rollback();
        } catch (SQLException e) {
          String formattedError = JdbcUtil.formatSqlException(e);
          LOG.error(formattedError);
          LOG.debug(formattedError, e);
        } finally {
          try {
            connection.close();
          } catch (SQLException e) {
            String formattedError = JdbcUtil.formatSqlException(e);
            LOG.error(formattedError);
            LOG.debug(formattedError, e);
          }
        }
      }
    }
    return errorRecords;
  }

  private Map<String, Object> getColumnMappings(Record record) {
    Map<String, Object> mappings = new HashMap<>(record.getFieldPaths().size());
    for (String fieldPath : record.getFieldPaths()) {
      if (fieldPath.isEmpty()) {
        continue;
      }

      final String fieldName = fieldPath.substring(1);
      if (!fieldName.startsWith("__")) {
        mappings.put(fieldPath.substring(1), record.get(fieldPath).getValue());
      }
    }

    return mappings;
  }
}
