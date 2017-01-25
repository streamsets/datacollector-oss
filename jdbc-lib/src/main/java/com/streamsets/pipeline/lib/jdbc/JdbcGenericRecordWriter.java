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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;

public class JdbcGenericRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcGenericRecordWriter.class);
  private final int maxPrepStmtCache;

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
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldColumnParamMapping> customMappings,
      int maxStmtCache,
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      JdbcRecordReader recordReader
  ) throws StageException {
    super(connectionString, dataSource, tableName, rollbackOnError,
        customMappings, defaultOp, unsupportedAction, recordReader, null);
    this.maxPrepStmtCache = maxStmtCache;
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
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldColumnParamMapping> customMappings,
      int maxStmtCache,
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      JdbcRecordReader recordReader
  ) throws StageException {
    super(connectionString, dataSource, tableName, rollbackOnError,
        customMappings, defaultOp, unsupportedAction, recordReader, generatedColumnMappings);
    this.maxPrepStmtCache = maxStmtCache;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    PreparedStatementMap statementsForBatch = null;
    try {
      connection = getDataSource().getConnection();

      statementsForBatch = new PreparedStatementMap(
          connection,
          getTableName(),
          getGeneratedColumnMappings(),
          getPrimaryKeyColumns(),
          maxPrepStmtCache
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
            getColumnsToFields()
        );
        PreparedStatement statement = null;
        try {
          statement = statementsForBatch.getPreparedStatement(
              opCode,
              columnsToParameters
          );
          setParameters(opCode, columnsToParameters, record, connection, statement);
          LOG.debug("Bound Query: {}", statement.toString());
          statement.execute();

          if (getGeneratedColumnMappings() != null) {
            writeGeneratedColumns(statement, batch.iterator(), errorRecords);
          }
        } catch (SQLException e) {
          if (getRollbackOnError()) {
            LOG.debug("Error due to {}. Rollback the record: {}", e.getMessage(), record.toString());
            connection.rollback();
          }
          errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_34, e.getMessage(), e.getCause()));
        } catch (OnRecordErrorException ex){
          errorRecords.add(ex);
        }
      }
      connection.commit();
    } catch (SQLException e) {
      handleSqlException(e);
    } finally {
      if (statementsForBatch != null) {
        // PreparedStatements for this tableName are cached. Close all.
        statementsForBatch.destroy();
      }
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

}
