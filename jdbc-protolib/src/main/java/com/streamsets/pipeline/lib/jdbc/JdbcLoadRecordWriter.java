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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * JDBC RecordWriter for LOAD operation only. Other operations will be treated
 * as error records. Currently only MySQL format is supported, and it executes
 * LOAD DATA LOCAL INFILE query using Apache CSVPrinter.
 */
public class JdbcLoadRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLoadRecordWriter.class);

  /** Action to take for duplicate-key errors */
  private final DuplicateKeyAction duplicateKeyAction;

  /** Single thread executor to write LOAD output stream */
  private final ExecutorService loadOutputExecutor;

  /**
   * Class constructor
   *
   * @param connectionString database connection string
   * @param dataSource JDBC {@link DataSource} to get a connection from
   * @param schema schema name
   * @param tableName table name
   * @param customMappings any custom mappings the user provided
   * @param duplicateKeyAction action to take for duplicate-key errors
   * @param recordReader base JdbcRecordReader, no CDC support
   * @param caseSensitive indicate whether to enclose the table name or not
   * @throws StageException
   */
  public JdbcLoadRecordWriter(
      String connectionString,
      DataSource dataSource,
      String schema,
      String tableName,
      List<JdbcFieldColumnParamMapping> customMappings,
      DuplicateKeyAction duplicateKeyAction,
      JdbcRecordReader recordReader,
      boolean caseSensitive,
      List<String> customDataSqlStateCodes
  ) throws StageException {
    super(
        connectionString,
        dataSource,
        schema,
        tableName,
        false, // No rollback support
        customMappings,
        OperationType.LOAD_CODE,
        UnsupportedOperationAction.SEND_TO_ERROR,
        recordReader,
        null,
        caseSensitive,
        customDataSqlStateCodes
    );
    this.duplicateKeyAction = duplicateKeyAction;
    String threadName = "JDBC LOAD DATA Stream " + getTableName();
    loadOutputExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat(threadName).build());
  }

  @Override
  public void deinit() {
    loadOutputExecutor.shutdown();
  }

  @Override
  public List<OnRecordErrorException> writePerRecord(
      Iterator<Record> recordIterator) throws StageException {
    throw new UnsupportedOperationException("JdbcLoadRecordWriter supports only batch write.");
  }

  @Override
  public List<OnRecordErrorException> writeBatch(
      Iterator<Record> recordIterator) throws StageException {
    final List<OnRecordErrorException> errorRecords = new LinkedList<>();
    if (!recordIterator.hasNext()) {
      return errorRecords;
    }

    // Assume all records have the same columns.
    final Record first = recordIterator.next();
    SortedMap<String, String> columnsToParameters = recordReader.getColumnsToParameters(
        first,
        OperationType.LOAD_CODE,
        getColumnsToParameters(),
        getColumnsToFields()
    );
    if (columnsToParameters.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_22);
    }

    final Set<String> columnNames = columnsToParameters.keySet();
    final String loadSql = "LOAD DATA LOCAL INFILE '' " + duplicateKeyAction.getKeyword()
        + " INTO TABLE " + getTableName() + " (" + Joiner.on(", ").join(columnNames) + ")";
    try (Connection connection = getDataSource().getConnection()) {
      Connection conn = connection.unwrap(Connection.class);
      try (PreparedStatement statement = conn.prepareStatement(loadSql)) {
        PipedInputStream is = new PipedInputStream();
        PipedOutputStream os = new PipedOutputStream(is);
        statement.getClass().getMethod("setLocalInfileInputStream", InputStream.class).invoke(statement, is);

        Future<?> future = loadOutputExecutor.submit(() -> {
          try (OutputStreamWriter writer = new OutputStreamWriter(os)) {
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.MYSQL);
            Record record = first;
            while (record != null) {
              int opCode = getOperationCode(record, errorRecords);
              if (opCode == OperationType.LOAD_CODE) {
                for (String column : columnNames) {
                  Field field = record.get(getColumnsToFields().get(column));
                  printer.print(field.getValue());
                }
                printer.println();
              } else if (opCode > 0) {
                LOG.debug("Sending record to error due to unsupported operation {}", opCode);
                errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_70, opCode));
              } else {
                // It should be added to the error records.
              }
              record = recordIterator.hasNext() ? recordIterator.next() : null;
            };
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });

        if (LOG.isDebugEnabled()) {
          LOG.debug("Executing query: {}", statement.toString());
        }
        statement.execute();
        future.get();
      }
      connection.commit();
    } catch (SQLException e) {
      handleSqlException(e);
    } catch (Exception e) {
      throw new StageException(JdbcErrors.JDBC_58, e.getMessage(), e);
    }
    return errorRecords;
  }
}
