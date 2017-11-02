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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.zaxxer.hikari.HikariDataSource;

import java.util.List;

public final class JdbcRecordReaderWriterFactory {

  private JdbcRecordReaderWriterFactory(){}

  // Called by JdbcTarget
  public static JdbcRecordWriter createJdbcRecordWriter(
      String connectionString,
      HikariDataSource dataSource,
      String schema,
      String tableName,
      List<JdbcFieldColumnParamMapping> customMappings,
      boolean rollbackOnError,
      boolean useMultiRowOp,
      int maxPrepStmtParameters,
      int maxPrepStmtCache,
      JDBCOperationType defaultOperation,
      UnsupportedOperationAction unsupportedAction,
      JdbcRecordReader recordReader,
      boolean caseSensitive
  ) throws StageException {

    return createJdbcRecordWriter(
        connectionString,
        dataSource,
        schema,
        tableName,
        customMappings,
        null,
        rollbackOnError,
        useMultiRowOp,
        maxPrepStmtParameters,
        maxPrepStmtCache,
        defaultOperation,
        unsupportedAction,
        recordReader,
        caseSensitive
    );
  }

  // Called by JdbcTeeProcessor
  public static JdbcRecordWriter createJdbcRecordWriter(
       String connectionString,
       HikariDataSource dataSource,
       String schema,
       String tableName,
       List<JdbcFieldColumnParamMapping> customMappings,
       List<JdbcFieldColumnMapping> generatedColumnMappings,
       boolean rollbackOnError,
       boolean useMultiRowOp,
       int maxPrepStmtParameters,
       int maxPrepStmtCache,
       JDBCOperationType defaultOperation,
       UnsupportedOperationAction unsupportedAction,
       JdbcRecordReader recordReader,
       boolean caseSensitive
  ) throws StageException {

    JdbcRecordWriter recordWriter;

    if (useMultiRowOp) {
      recordWriter = new JdbcMultiRowRecordWriter(
          connectionString,
          dataSource,
          schema,
          tableName,
          rollbackOnError,
          customMappings,
          maxPrepStmtParameters,
          defaultOperation,
          unsupportedAction,
          generatedColumnMappings,
          recordReader,
          caseSensitive
      );
    } else {
      recordWriter = new JdbcGenericRecordWriter(
          connectionString,
          dataSource,
          schema,
          tableName,
          rollbackOnError,
          customMappings,
          maxPrepStmtCache,
          defaultOperation,
          unsupportedAction,
          generatedColumnMappings,
          recordReader,
          caseSensitive
      );
    }
    return recordWriter;
  }

  public static JdbcRecordReader createRecordReader(ChangeLogFormat changeLogFormat){
    JdbcRecordReader recordReader;
    switch (changeLogFormat) {
      case MSSQL:
        recordReader = new JdbcMicrosoftRecordReader();
        break;
      case OracleCDC:
        recordReader = new JdbcOracleCDCRecordReader();
        break;
      case NONE:
        recordReader = new JdbcRecordReader();
        break;
      case MySQLBinLog:
        recordReader = new JdbcMySqlBinLogRecordReader();
        break;
      case MongoDBOpLog:
        recordReader = new JdbcMongoDBOplogRecordReader();
        break;
      default:
        throw new IllegalStateException("Unrecognized format specified: " + changeLogFormat);
    }
    return recordReader;
  }
}
