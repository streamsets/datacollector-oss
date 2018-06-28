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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_14;

public class JdbcGenericRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcGenericRecordWriter.class);
  private final int maxPrepStmtCache;
  private final boolean caseSensitive;

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link javax.sql.DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @param defaultOp Default Opertaion
   * @param unsupportedAction What action to take if operation is invalid
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
      int maxStmtCache,
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      JdbcRecordReader recordReader,
      boolean caseSensitive
  ) throws StageException {
    super(connectionString, dataSource, schema, tableName, rollbackOnError,
        customMappings, defaultOp, unsupportedAction, recordReader, null, caseSensitive);
    this.maxPrepStmtCache = maxStmtCache;
    this.caseSensitive = caseSensitive;
  }

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link javax.sql.DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @param defaultOp Default Opertaion
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
      int maxStmtCache,
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      JdbcRecordReader recordReader,
      boolean caseSensitive
  ) throws StageException {
    super(connectionString, dataSource, schema, tableName, rollbackOnError,
        customMappings, defaultOp, unsupportedAction, recordReader, generatedColumnMappings, caseSensitive);
    this.maxPrepStmtCache = maxStmtCache;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public List<OnRecordErrorException> writePerRecord(Collection<Record> batch) throws StageException {
    final boolean perRecord = true;
    return write(batch, perRecord);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
    final boolean perRecord = false;
    return write(batch, perRecord);
  }

  /**
   * write the batch of the records if it is not perRecord
   * otherwise, execute one statement per record
   * @param batch
   * @param perRecord
   * @return List<OnRecordErrorException>
   * @throws StageException
   */
  private List<OnRecordErrorException> write(Collection<Record> batch, boolean perRecord) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    PreparedStatementMap statementsForBatch = null;
    List<PreparedStatement> statementsToExecute = new ArrayList<>();
    // Map that keeps list of records that has been used for each statement -- for error handling
    Map<PreparedStatement, List<Record>> statementsToRecords  = new HashMap<>();
    try {
      connection = getDataSource().getConnection();

      statementsForBatch = new PreparedStatementMap(
          connection,
          getTableName(),
          getGeneratedColumnMappings(),
          getPrimaryKeyColumns(),
          maxPrepStmtCache,
          caseSensitive
      );

      for (Record record : batch) {
        // First, find the operation code
        int opCode = recordReader.getOperationFromRecord(record, defaultOp, unsupportedAction, errorRecords);
        if (opCode <= 0) {
          continue;
        }
        // columnName to parameter mapping. Ex. parameter is default "?".
        SortedMap<String, String> columnsToParameters = recordReader.getColumnsToParameters(
            record,
            opCode,
            getColumnsToParameters(),
            opCode == OperationType.UPDATE_CODE ? getColumnsToFieldNoPK() : getColumnsToFields()
        );

        if (columnsToParameters.isEmpty()) {
          // no parameters found for configured columns
          if (LOG.isWarnEnabled()) {
            LOG.warn("No parameters found for record with ID {}; skipping", record.getHeader().getSourceId());
          }
          continue;
        }

        PreparedStatement statement;
        try {
          statement = statementsForBatch.getPreparedStatement(opCode, columnsToParameters);
          statementsToRecords.computeIfAbsent(statement, (key) -> new ArrayList<>()).add(record);

          setParameters(opCode, columnsToParameters, record, connection, statement);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Bound Query: {}", statement.toString());
          }

          if (!perRecord) {
            statement.addBatch();

            if (!statementsToExecute.contains(statement)) {
              statementsToExecute.add(statement);
            }
          } else {
            statement.executeUpdate();

            if (getGeneratedColumnMappings() != null) {
              writeGeneratedColumns(statement, Arrays.asList(record).iterator(), errorRecords);
            }
          }
        } catch (SQLException ex) { // These don't trigger a rollback
          errorRecords.add(new OnRecordErrorException(record, JDBC_14, ex));
        } catch (OnRecordErrorException ex) {
          errorRecords.add(ex);
        }
      }

      if (!perRecord) {
        for (PreparedStatement statement : statementsToExecute) {
          try {
            statement.executeBatch();
          } catch(SQLException e){
            if (getRollbackOnError()) {
              connection.rollback();
            }
            handleBatchUpdateException(statementsToRecords.get(statement), e, errorRecords);
          }

          if (getGeneratedColumnMappings() != null) {
            writeGeneratedColumns(statement, batch.iterator(), errorRecords);
          }
        }
      }

      connection.commit();
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
      SortedMap<String, String> columnsToParameters,
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
    if (JdbcUtil.isDataError(getConnectionString(), e)) {
      String formattedError = JdbcUtil.formatSqlException(e);
      LOG.error(formattedError);
      LOG.debug(formattedError, e);

      if (!getRollbackOnError() && e instanceof BatchUpdateException &&
          ((BatchUpdateException) e).getUpdateCounts().length > 0) {
        BatchUpdateException bue = (BatchUpdateException) e;

        int i = 0;
        for (Record record : failedRecords) {
          if (i >= bue.getUpdateCounts().length || bue.getUpdateCounts()[i] == PreparedStatement.EXECUTE_FAILED) {
            errorRecords.add(new OnRecordErrorException(record, JDBC_14, formattedError));
          }
          i++;
        }
      } else {
        for (Record record : failedRecords) {
          errorRecords.add(new OnRecordErrorException(record, JDBC_14, formattedError));
        }
      }
    } else {
      handleSqlException(e);
    }
  }
}
