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

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_14;
import static com.streamsets.pipeline.lib.operation.OperationType.DELETE_CODE;
import static com.streamsets.pipeline.lib.operation.OperationType.INSERT_CODE;

public class JdbcGenericRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcGenericRecordWriter.class);
  private final boolean caseSensitive;
  private final boolean sortedColumns;
  private final Timer queryTimer;
  private final Timer commitTimer;

  private static final HashFunction columnHashFunction = Hashing.goodFastHash(64);
  private static final Funnel<Map<String, String>> stringMapFunnel = (map, into) -> {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      into.putString(entry.getKey(), Charsets.UTF_8).putString(entry.getValue(), Charsets.UTF_8);
    }
  };

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link javax.sql.DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @param defaultOpCode default operation code
   * @param unsupportedAction What action to take if operation is invalid
   * @param generatedColumnMappings mappings from field names to generated column names
   * @param recordReader JDBCRecordReader to obtain data from incoming record
   * @throws StageException
   */
  public JdbcGenericRecordWriter(
      String connectionString,
      DataSource dataSource,
      String schema,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldColumnParamMapping> customMappings,
      int defaultOpCode,
      UnsupportedOperationAction unsupportedAction,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      JdbcRecordReader recordReader,
      boolean caseSensitive,
      List<String> customDataSqlStateCodes,
      boolean sortedColumns,
      Context context
  ) throws StageException {
    super(
        connectionString,
        dataSource,
        schema,
        tableName,
        rollbackOnError,
        customMappings,
        defaultOpCode,
        unsupportedAction,
        recordReader,
        generatedColumnMappings,
        caseSensitive,
        customDataSqlStateCodes
    );
    this.caseSensitive = caseSensitive;
    this.sortedColumns = sortedColumns;
    this.queryTimer = context.createTimer("Query Timer");
    this.commitTimer = context.createTimer("Commit Timer");
  }

  @Override
  public List<OnRecordErrorException> writePerRecord(Iterator<Record> recordIterator) throws StageException {
    return write(recordIterator, true);
  }

  @Override
  public List<OnRecordErrorException> writeBatch(Iterator<Record> recordIterator) throws StageException {
    return write(recordIterator, false);
  }

  public List<OnRecordErrorException> write(Iterator<Record> recordIterator, boolean perRecord) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();

    try (Connection connection = getDataSource().getConnection()){
      int prevOpCode = -1;
      HashCode prevColumnHash = null;
      LinkedList<Pair<Record, Map<String, String>>> queue = new LinkedList<>();

      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();
        int opCode = getOperationCode(record, errorRecords);

        Map<String, String> columnsToParameters = recordReader.getColumnsToParameters(
            record,
            opCode,
            getColumnsToParameters(),
            opCode == OperationType.UPDATE_CODE ? getColumnsToFieldNoPK() : getColumnsToFields(),
            sortedColumns
        );

        // Need to consider the number of columns in query. If different, process saved records in queue.
        HashCode columnHash = columnHashFunction.newHasher().putObject(columnsToParameters, stringMapFunnel).hash();

        boolean opCodeValid = opCode > 0;
        boolean opCodeUnchanged = opCode == prevOpCode;
        boolean supportedOpCode = opCode == DELETE_CODE || opCode == INSERT_CODE && columnHash.equals(prevColumnHash);
        boolean canEnqueue = opCodeValid && opCodeUnchanged && supportedOpCode;

        if (canEnqueue) {
          queue.add(Pair.of(record, columnsToParameters));
        }

        if (!opCodeValid || canEnqueue) {
          continue;
        }

        // Process enqueued records.
        processQueue(queue, errorRecords, connection, prevOpCode, perRecord);

        if (!queue.isEmpty()) {
          throw new IllegalStateException(Utils.format("Queue processed, but was not empty upon completion ({} remaining items).", queue.size()));
        }

        queue.add(Pair.of(record, columnsToParameters));
        prevOpCode = opCode;
        prevColumnHash = columnHash;
      }


      // Check if any records are left in queue unprocessed
      processQueue(queue, errorRecords, connection, prevOpCode, perRecord);
      try(Timer.Context t = commitTimer.time()) {
        connection.commit();
      }
    } catch (SQLException e) {
      handleSqlException(e);
    }

    return errorRecords;
  }

  private void processQueue(
      LinkedList<Pair<Record, Map<String, String>>> queue,
      List<OnRecordErrorException> errorRecords,
      Connection connection,
      int opCode,
      boolean perRecord
  ) throws StageException {
    if (queue.isEmpty()) {
      return;
    }

    PreparedStatement statement = null;
    LinkedList<Record> queueRecord = new LinkedList<>();

    for(Pair<Record, Map<String, String>> recordValue : queue) {
      if (opCode <= 0) {
        continue;
      }

      Record record = recordValue.getKey();
      Map<String, String> columnsToParameters = recordValue.getValue();
      queueRecord.add(record);

      if (columnsToParameters.isEmpty()) {
        errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_90, getTableName()));
        continue;
      }

      try {
        // Generate new statement only if the old one is null (could happen in processing records row-by-row)
        if(statement == null) {
          statement = jdbcUtil.getPreparedStatement(
              getGeneratedColumnMappings(),
              generateQuery(opCode, columnsToParameters, record),
              connection
          );
        }

        setParameters(opCode, columnsToParameters, record, connection, statement);

        executeStatement(statement, perRecord);

        if (perRecord) {
          if (getGeneratedColumnMappings() != null) {
            writeGeneratedColumns(statement, Arrays.asList(record).iterator(), errorRecords);
          }
          statement = null;
        }
      } catch (SQLException ex) { // These don't trigger a rollback
        errorRecords.add(new OnRecordErrorException(
            record,
            JDBC_14,
            ex.getSQLState(),
            ex.getErrorCode(),
            ex.getMessage(),
            jdbcUtil.formatSqlException(ex),
            ex
        ));
      } catch (OnRecordErrorException ex) {
        errorRecords.add(ex);
      }
    }

    try {
      if (!perRecord && statement != null) {
        try {
          try(Timer.Context t = queryTimer.time()) {
            statement.executeBatch();
          }
        } catch(SQLException e){
          if (getRollbackOnError()) {
            connection.rollback();
          }
          handleBatchUpdateException(queueRecord, e, errorRecords);
        }

        if (getGeneratedColumnMappings() != null) {
          writeGeneratedColumns(statement, queueRecord.iterator(), errorRecords);
        }
      }

      try(Timer.Context t = commitTimer.time()) {
        connection.commit();
      }
    } catch (SQLException e) {
      handleSqlException(e);
    }

    queue.clear();
  }

  protected void executeStatement(PreparedStatement statement, boolean perRecord) throws SQLException {
    if (!perRecord) {
      statement.addBatch();
    } else {
      try(Timer.Context t = queryTimer.time()) {
        statement.executeUpdate();
      }
    }
  }

  /**
   * Set parameters and primary keys in query.
   * @param opCode
   * @param columnsToParameters
   * @param record
   * @param connection
   * @param statement
   * @return
   */
  @VisibleForTesting
  @SuppressWarnings("unchecked")
  int setParameters(
      int opCode,
      Map<String, String> columnsToParameters,
      final Record record,
      final Connection connection,
      PreparedStatement statement
  ) throws OnRecordErrorException {
    int paramIdx = 1;

    // Set columns and their value in query. No need to perform this for delete operation.
    if(opCode != OperationType.DELETE_CODE) {
      paramIdx = setParamsToStatement(paramIdx, statement, columnsToParameters, record, connection, opCode);
    }
    // Set primary keys in WHERE clause for update and delete operations
    if(opCode != OperationType.INSERT_CODE){
      paramIdx = setPrimaryKeys(paramIdx, record, statement, opCode);
    }
    return paramIdx;
  }

  /**
   * <p>
   *   Some databases drivers allow us to figure out which record in a particular batch failed.
   * </p>
   * <p>
   *   In the case that we have a list of update counts, we can mark just the record as erroneous.
   *   Otherwise we must send the entire batch to error.
   * </p>
   * @param failedRecords List of Failed Records
   * @param e BatchUpdateException
   * @param errorRecords List of error records for this batch
   */
  private void handleBatchUpdateException(
      Collection<Record> failedRecords, SQLException e, List<OnRecordErrorException> errorRecords
  ) throws StageException {
    if (jdbcUtil.isDataError(getCustomDataSqlStateCodes(), getConnectionString(), e)) {
      String formattedError = JdbcErrors.JDBC_79.getMessage();
      LOG.error(formattedError);
      LOG.debug(formattedError, e);

      if (!getRollbackOnError() && e instanceof BatchUpdateException &&
          ((BatchUpdateException) e).getUpdateCounts().length > 0) {
        BatchUpdateException bue = (BatchUpdateException) e;

        int i = 0;
        for (Record record : failedRecords) {
          if (i >= bue.getUpdateCounts().length || bue.getUpdateCounts()[i] == PreparedStatement.EXECUTE_FAILED) {
            errorRecords.add(new OnRecordErrorException(
                record,
                JDBC_14,
                e.getSQLState(),
                e.getErrorCode(),
                e.getMessage(),
                jdbcUtil.formatSqlException(e),
                e
            ));
          }
          i++;
        }
      } else {
        for (Record record : failedRecords) {
          errorRecords.add(new OnRecordErrorException(
              record,
              JDBC_14,
              e.getSQLState(),
              e.getErrorCode(),
              e.getMessage(),
              jdbcUtil.formatSqlException(e),
              e
          ));
        }
      }
    } else {
      handleSqlException(e);
    }
  }

  private String generateQuery(
      int opCode,
      final Map<String, String> columns,
      final Record record
  ) throws OnRecordErrorException {
    List<String> primaryKeyParams = new LinkedList<>();
    for (String key: getPrimaryKeyColumns()) {
      primaryKeyParams.add(columns.get(key));
    }

    final int recordSize = 1;
    String query = jdbcUtil.generateQuery(opCode, getTableName(), getPrimaryKeyColumns(), primaryKeyParams, columns, recordSize, caseSensitive, false, record);
    LOG.debug("Generated query:" + query);
    return query;
  }
}
