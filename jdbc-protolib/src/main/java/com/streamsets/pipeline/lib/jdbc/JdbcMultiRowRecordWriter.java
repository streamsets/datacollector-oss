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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.streamsets.pipeline.lib.operation.OperationType.DELETE_CODE;
import static com.streamsets.pipeline.lib.operation.OperationType.INSERT_CODE;

public class JdbcMultiRowRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcMultiRowRecordWriter.class);

  private static final HashFunction columnHashFunction = Hashing.goodFastHash(64);
  private static final Funnel<Map<String, String>> stringMapFunnel = (map, into) -> {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      into.putString(entry.getKey(), Charsets.UTF_8).putString(entry.getValue(), Charsets.UTF_8);
    }
  };

  public static final int UNLIMITED_PARAMETERS = -1;
  private final boolean caseSensitive;
  private int maxPrepStmtParameters;
  private final Timer queryTimer;
  private final Timer commitTimer;

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @param maxPrepStmtParameters max number of parameters to include in each INSERT statement
   * @param defaultOpCode default operation code
   * @param unsupportedAction What action to take if operation is invalid
   * @param generatedColumnMappings mappings from field names to generated column names
   * @throws StageException
   */
  public JdbcMultiRowRecordWriter(
      String connectionString,
      DataSource dataSource,
      String schema,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldColumnParamMapping> customMappings,
      int maxPrepStmtParameters,
      int defaultOpCode,
      UnsupportedOperationAction unsupportedAction,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      JdbcRecordReader recordReader,
      boolean caseSensitive,
      List<String> customDataSqlStateCodes,
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
    this.maxPrepStmtParameters = maxPrepStmtParameters == UNLIMITED_PARAMETERS ? Integer.MAX_VALUE :
        maxPrepStmtParameters;
    this.caseSensitive = caseSensitive;
    this.queryTimer = context.createTimer("Query Timer");
    this.commitTimer = context.createTimer("Commit Timer");
  }

  @Override
  public List<OnRecordErrorException> writePerRecord(Iterator<Record> recordIterator) throws StageException {
    throw new UnsupportedOperationException("Multiple Row Record Writer operation is not supported to SQL Server");
  }


  @Override
  public List<OnRecordErrorException> writeBatch(Iterator<Record> recordIterator) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      // compute number of rows per batch
      if (getColumnsToParameters().isEmpty()) {
        throw new StageException(JdbcErrors.JDBC_22);
      }

      int maxRowsPerBatch = maxPrepStmtParameters / getColumnsToParameters().size();
      int prevOpCode = -1;
      HashCode prevColumnHash = null;
      // put all the records with the same operation in a queue to create a multi-row query
      LinkedList<Record> queue = new LinkedList<>();
      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();
        int opCode = getOperationCode(record, errorRecords);

        // Need to consider the number of columns in query. If different, process saved records in queue.
        HashCode columnHash = getColumnHash(record, opCode);

        boolean opCodeValid = opCode > 0;
        boolean opCodeUnchanged = opCode == prevOpCode;
        boolean supportedOpCode = opCode == DELETE_CODE || opCode == INSERT_CODE && columnHash.equals(prevColumnHash);
        boolean canEnqueue = opCodeValid && opCodeUnchanged && supportedOpCode;

        if (canEnqueue) {
          queue.add(record);
        }

        if (!opCodeValid || canEnqueue) {
          continue;
        }

        // Process enqueued records.
        processQueue(queue, errorRecords, connection, maxRowsPerBatch, prevOpCode);

        if (!queue.isEmpty()) {
          throw new IllegalStateException(Utils.format("Queue processed, but was not empty upon completion ({} remaining items).", queue.size()));
        }

        queue.add(record);
        prevOpCode = opCode;
        prevColumnHash = columnHash;
      }


      // Check if any records are left in queue unprocessed
      processQueue(queue, errorRecords, connection, maxRowsPerBatch, prevOpCode);
      try(Timer.Context t = commitTimer.time()) {
        connection.commit();
      }
    } catch (SQLException e) {
      handleSqlException(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          handleSqlException(e);
        }
      }
    }
    return errorRecords;
  }

  /**
   * Process all records in queue. All records have same operation to same table.
   * Generate a query and set parameters from each record. INSERT and DELETE can be multi-row operation
   * but UPDATE is single-row operation.
   * If maxStatement
   * @param errorRecords
   * @param connection
   * @param maxRowsPerBatch
   * @param opCode
   * @param queue
   * @return
   * @throws OnRecordErrorException
   */
  private void processQueue(
      LinkedList<Record> queue,
      List<OnRecordErrorException> errorRecords,
      Connection connection,
      int maxRowsPerBatch,
      int opCode
  ) throws StageException {
    if (queue.isEmpty()) {
      return;
    }

    int rowCount = 0;
    // Assume that columns are all same for the same operation to the same table
    // If some columns are missing in record, the record goes to error.
    final Record first = queue.getFirst();
    SortedMap<String, String> columnsToParameters = recordReader.getColumnsToParameters(
        first,
        opCode,
        getColumnsToParameters(),
        opCode == OperationType.UPDATE_CODE ? getColumnsToFieldNoPK() : getColumnsToFields()
    );

    // Neither of the records have schema matching the table, so we will move all records to error stream
    if (columnsToParameters.isEmpty()) {
      addRecordErrors(errorRecords, queue, JdbcErrors.JDBC_90);
      return;
    }

    if(opCode != OperationType.INSERT_CODE && getPrimaryKeyColumns().isEmpty()){
      LOG.error("Primary key columns are missing in records: {}", getPrimaryKeyColumns());
      addRecordErrors(errorRecords, queue, JdbcErrors.JDBC_62);
      return;
    }

    String query = generateQueryForMultiRow(
        opCode,
        columnsToParameters,
        getPrimaryKeyColumns(),
        // the next batch will have either the max number of records, or however many are left.
        Math.min(maxRowsPerBatch, queue.size())
    );

    // Need to store removed records from queue, because we might need to add newly generated columns
    // to records for Jdbc Tee Processor.
    LinkedList<Record> removed = new LinkedList<>();

    try (PreparedStatement statement = jdbcUtil.getPreparedStatement(getGeneratedColumnMappings(), query, connection)) {
      int paramIdx = 1;
      // Start processing records in queue. All records have the same operation to the same table.
      while (!queue.isEmpty()) {
        Record r = queue.removeFirst();
        if (opCode != DELETE_CODE) {
          paramIdx = setParamsToStatement(paramIdx, statement, columnsToParameters, r, connection, opCode);
        }
        if (opCode != OperationType.INSERT_CODE) {
          paramIdx = setPrimaryKeys(paramIdx, r, statement, opCode);
        }
        removed.add(r);
        ++rowCount;
        if (rowCount == maxRowsPerBatch) {
          // time to execute the current batch
          processBatch(removed, errorRecords, statement, connection);
          // reset our counters
          rowCount = 0;
          paramIdx = 1;
          removed.clear();
        }
      }
    } catch (SQLException e) {
      handleSqlException(e, removed, errorRecords);
    }

    // Process the rest of the records that are removed from queue but haven't processed yet
    // this happens when rowCount is still less than maxRowsPerBatch.
    // This is a bit of an ugly fix as its not very DRY but sufficient until there's a larger
    // refactoring of this code.
    if (!removed.isEmpty()) {
      query = generateQueryForMultiRow(
          opCode,
          columnsToParameters,
          getPrimaryKeyColumns(),
          removed.size() // always the remainder
      );

      try (PreparedStatement statement = jdbcUtil.getPreparedStatement(
          getGeneratedColumnMappings(),
          query,
          connection
      )) {
        int paramIdx = 1;
        for (Record r : removed) {
          if (opCode != DELETE_CODE) {
            paramIdx = setParamsToStatement(paramIdx, statement, columnsToParameters, r, connection, opCode);
          }
          if (opCode != OperationType.INSERT_CODE) {
            paramIdx = setPrimaryKeys(paramIdx, r, statement, opCode);
          }
        }
        processBatch(removed, errorRecords, statement, connection);
      } catch (SQLException e) {
        handleSqlException(e, removed, errorRecords);
      }
    }
  }

  private void addRecordErrors(
      final List<OnRecordErrorException> errorRecords,
      final LinkedList<Record> queue,
      final JdbcErrors error
  ) {
    while(!queue.isEmpty()) {
      Record record = queue.removeFirst();
      errorRecords.add(new OnRecordErrorException(record, error, getTableName()));
    }
  }

  private void processBatch(
      LinkedList<Record> queue,
      List<OnRecordErrorException> errorRecords,
      PreparedStatement statement,
      Connection connection
  ) throws SQLException {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing query: {}", statement.toString());
      }
      try(Timer.Context t = queryTimer.time()) {
        statement.executeUpdate();
      }
    } catch (SQLException ex) {
      if (getRollbackOnError()) {
        LOG.debug("Error due to {}. Rollback the batch.", ex.getMessage());
        connection.rollback();
      }
      throw ex;
    }

    if (getGeneratedColumnMappings() != null) {
      writeGeneratedColumns(statement, queue.iterator(), errorRecords);
    }
  }

  /**
   * Handle SQLException in a smart way, detecting if the exception is data oriented or not.
   */
  private void handleSqlException(
    SQLException exception,
    List<Record> inputRecords,
    List<OnRecordErrorException> errors
  ) throws StageException {
    if(jdbcUtil.isDataError(getCustomDataSqlStateCodes(), getConnectionString(), exception)) {
      String formattedError = jdbcUtil.formatSqlException(exception);
      LOG.error(JdbcErrors.JDBC_89.getMessage(), formattedError);

      for(Record inputRecord : inputRecords) {
        errors.add(new OnRecordErrorException(inputRecord, JdbcErrors.JDBC_89, formattedError));
      }
      return;
    }

    super.handleSqlException(exception);
  }

  @VisibleForTesting
  String generateQueryForMultiRow(
      int opCode,
      SortedMap<String, String> columns,
      List<String> primaryKeys,
      int numRecords
  ) throws OnRecordErrorException {

    // generateQueryForMultiRow can throw an error if the op code is invalid, but processQueue calling generateQueryForMultiRow
    // is called only if the op code is supported, so any value inlcuding null will not be used for the record (the last) param in this call.
    String query = jdbcUtil.generateQuery(opCode, getTableName(), primaryKeys, getPrimaryKeyParams(), columns, numRecords, caseSensitive, true, null);

    LOG.debug("Generated multi-row operation query: {}", query);
    return query;
  }

  /**
   * Generates a hash for the fields present in a record and their mappings.
   * A specific implementation of the hash function is not guaranteed.
   *
   * @param record The record to generate a hash for.
   * @return A Guava HashCode of the fields.
   */
  private HashCode getColumnHash(Record record, int op) throws OnRecordErrorException {
    Map<String, String> parameters = getColumnsToParameters();
    SortedMap<String, String> columnsToParameters
        = recordReader.getColumnsToParameters(record, op, parameters, getColumnsToFields());
    return columnHashFunction.newHasher().putObject(columnsToParameters, stringMapFunnel).hash();
  }
}
