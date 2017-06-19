/**
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.Collection;
import java.util.TreeMap;

public class JdbcMultiRowRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcMultiRowRecordWriter.class);

  private static final HashFunction columnHashFunction = Hashing.goodFastHash(64);
  private static final Funnel<Map<String, String>> stringMapFunnel = new Funnel<Map<String, String>>() {
    @Override
    public void funnel(Map<String, String> map, PrimitiveSink into) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        into.putString(entry.getKey(), Charsets.UTF_8).putString(entry.getValue(), Charsets.UTF_8);
      }
    }
  };

  public static final int UNLIMITED_PARAMETERS = -1;
  private final boolean caseSensitive;
  private int maxPrepStmtParameters;

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @param maxPrepStmtParameters max number of parameters to include in each INSERT statement
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
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      JdbcRecordReader recordReader,
      boolean caseSensitive
  ) throws StageException {
    super(connectionString, dataSource, schema, tableName, rollbackOnError, customMappings, defaultOp, unsupportedAction, recordReader, caseSensitive);
    this.maxPrepStmtParameters = maxPrepStmtParameters == UNLIMITED_PARAMETERS ? Integer.MAX_VALUE :
        maxPrepStmtParameters;
    this.caseSensitive = caseSensitive;
  }

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @param maxPrepStmtParameters max number of parameters to include in each INSERT statement
   * @param defaultOp Default Opertaion
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
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      JdbcRecordReader recordReader,
      boolean caseSensitive
  ) throws StageException {
    super(connectionString, dataSource, schema, tableName, rollbackOnError, customMappings,
        defaultOp, unsupportedAction, recordReader, generatedColumnMappings, caseSensitive);
    this.maxPrepStmtParameters = maxPrepStmtParameters == UNLIMITED_PARAMETERS ? Integer.MAX_VALUE :
        maxPrepStmtParameters;
    this.caseSensitive = caseSensitive;
  }


  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
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
      for (Record record : batch) {
        int opCode = recordReader.getOperationFromRecord(record, defaultOp, unsupportedAction, errorRecords);
        if (opCode <= 0) { // sent to errorRecords
          errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_70, opCode));
          continue;
        }
        // Need to consider the number of columns in query. If different, process saved records in queue.
        HashCode columnHash = getColumnHash(record, opCode);
        if (prevOpCode == opCode && (opCode == OperationType.DELETE_CODE ||
              (opCode == OperationType.INSERT_CODE && columnHash.equals(prevColumnHash)))) {
          queue.add(record);
          continue;
        }
        // Execute the records in queue.
        if (!queue.isEmpty()) {
          processQueue(queue, errorRecords, connection, maxRowsPerBatch, prevOpCode);
        }
        queue.clear();
        queue.add(record);
        prevOpCode = opCode;
        prevColumnHash = columnHash;
      }


      // Check if any records are left in queue unprocessed
      if (!queue.isEmpty()) {
        processQueue(queue, errorRecords, connection, maxRowsPerBatch, prevOpCode);
      }
      connection.commit();
    } catch (SQLException e) {
      handleSqlException(e);
    } finally {
      if (connection != null) {
        try {
          connection.commit();
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
   * @throws SQLException
   */
  private void processQueue(
      LinkedList<Record> queue,
      List<OnRecordErrorException> errorRecords,
      Connection connection,
      int maxRowsPerBatch,
      int opCode
  ) throws StageException {
    int rowCount = 0;
    PreparedStatement statement = null;
    String query;
    //Assume that columns are all same for the same operation to the same table
    //If some columns are missing in record, the record goes to error.
    SortedMap<String, String> columnsToParameters = recordReader.getColumnsToParameters(
        queue.getFirst(),
        opCode,
        getColumnsToParameters(),
        getColumnsToFields()
    );
    try {
      int paramIdx = 1;
      // Need to store removed records from queue, because we might need to add newly generated columns
      // to records for Jdbc Tee Processor.
      LinkedList<Record> removed = new LinkedList<>();
      // Start processing records in queue. All records have the same operation to the same table.
      while (!queue.isEmpty()) {
        if (statement == null) {
          query = generateQueryForMultiRow(
              opCode,
              columnsToParameters,
              getPrimaryKeyColumns(),
              // the next batch will have either the max number of records, or however many are left.
              Math.min(maxRowsPerBatch, queue.size())
          );
          statement = JdbcUtil.getPreparedStatement(getGeneratedColumnMappings(), query, connection);
        }
        Record r = queue.removeFirst();
        if (opCode != OperationType.DELETE_CODE) {
          paramIdx = setParamsToStatement(paramIdx, statement, columnsToParameters, r, connection, opCode);
        }
        if (opCode != OperationType.INSERT_CODE) {
          paramIdx = setPrimaryKeys(paramIdx, r, statement, opCode);
        }
        rowCount++;
        removed.add(r);
        if (rowCount == maxRowsPerBatch) {
          // time to execute the current batch
          processBatch(removed, errorRecords, statement, connection);
          // reset our counters
          rowCount = 0;
          paramIdx = 1;
          removed.clear();
        }
      }
      // Process the rest of the records that are removed from queue but haven't processed yet
      // this happens when rowCount is still less than maxRowsPerBatch.
      if (rowCount != 0) {
        processBatch(removed, errorRecords, statement, connection);
      }
    } catch (SQLException e) {
      handleSqlException(e);
    }
  }

  private void processBatch(
      LinkedList<Record> queue,
      List<OnRecordErrorException> errorRecords,
      PreparedStatement statement,
      Connection connection) throws SQLException
  {
    try {
      LOG.debug("Executing query: " + statement.toString());
      statement.addBatch();
      statement.executeBatch();
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

  @VisibleForTesting
  String generateQueryForMultiRow(
      int opCode,
      SortedMap<String, String> columns,
      List<String> primaryKeys,
      int numRecords
  ) throws OnRecordErrorException, SQLException {

    String query = JdbcUtil.generateQuery(opCode, getTableName(), primaryKeys, getPrimaryKeyParams(), columns, numRecords, caseSensitive, true);

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
    if (columnsToParameters.isEmpty()){
      throw new OnRecordErrorException(JdbcErrors.JDBC_22);
    }
    return columnHashFunction.newHasher().putObject(columnsToParameters, stringMapFunnel).hash();
  }
}
