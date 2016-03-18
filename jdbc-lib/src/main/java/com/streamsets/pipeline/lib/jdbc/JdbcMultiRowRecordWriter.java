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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.jdbc.Errors;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcFieldMappingConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldMappingConfig> customMappings,
      int maxPrepStmtParameters)
      throws StageException
  {
    super(connectionString, dataSource, tableName, rollbackOnError, customMappings);
    this.maxPrepStmtParameters =
        maxPrepStmtParameters == UNLIMITED_PARAMETERS ? Integer.MAX_VALUE : maxPrepStmtParameters;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();

      // Since we are doing multi-row inserts we have to partition the batch into groups of the same
      // set of fields. We'll also sort each partition for optimal inserts into column stores.
      Multimap<Long, Record> partitions = partitionBatch(batch);

      for (Long partitionKey : partitions.keySet()) {
        processPartition(connection, partitions, partitionKey, errorRecords);
      }
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

  @SuppressWarnings("unchecked")
  private void processPartition(
      Connection connection,
      Multimap<Long, Record> partitions,
      Long partitionKey,
      List<OnRecordErrorException> errorRecords
  ) throws SQLException, OnRecordErrorException {
    Collection<Record> partition = partitions.get(partitionKey);
    // Fetch the base insert query for this partition.
    SortedMap<String, String> columnsToParameters = getFilteredColumnsToParameters(
        getColumnsToParameters(), partition.iterator().next()
    );

    // put all the records in a queue for consumption
    LinkedList<Record> queue = new LinkedList<>(partition);

    // compute number of rows per batch
    if (columnsToParameters.isEmpty()) {
      throw new OnRecordErrorException(Errors.JDBCDEST_22);
    }
    int maxRowsPerBatch = maxPrepStmtParameters / columnsToParameters.size();

    PreparedStatement statement = null;

    // parameters are indexed starting with 1
    int paramIdx = 1;
    int rowCount = 0;
    while (!queue.isEmpty()) {
      // we're at the start of a batch.
      if (statement == null) {
        // instantiate the new statement
        statement = generatePreparedStatement(
            columnsToParameters,
            // the next batch will have either the max number of records, or however many are left.
            Math.min(maxRowsPerBatch, queue.size()),
            getTableName(),
            connection
        );
      }

      // process the next record into the current statement
      Record record = queue.removeFirst();
      for (String column : columnsToParameters.keySet()) {
        Field field = record.get(getColumnsToFields().get(column));
        Field.Type fieldType = field.getType();
        Object value = field.getValue();

        try {
          switch (fieldType) {
            case LIST:
              List<Object> unpackedList = unpackList((List<Field>) value);
              Array array = connection.createArrayOf(getSQLTypeName(fieldType), unpackedList.toArray());
              statement.setArray(paramIdx, array);
              break;
            case DATE:
            case DATETIME:
              // Java Date types are not accepted by JDBC drivers, so we need to convert to java.sql.Date
              java.util.Date date = field.getValueAsDatetime();
              statement.setObject(paramIdx, new java.sql.Date(date.getTime()));
              break;
            default:
              statement.setObject(paramIdx, value, getColumnType(column));
              break;
          }
        } catch (SQLException e) {
          LOG.error(Errors.JDBCDEST_23.getMessage(), column, fieldType.toString(), e);
          throw new OnRecordErrorException(record, Errors.JDBCDEST_23, column, fieldType.toString());
        }
        ++paramIdx;
      }

      rowCount++;

      // check if we've filled up the current batch
      if (rowCount == maxRowsPerBatch) {
        // time to execute the current batch
        statement.addBatch();
        statement.executeBatch();
        statement.close();
        statement = null;

        // reset our counters
        rowCount = 0;
        paramIdx = 1;
      }
    }

    // check if there are any records left. this should occur whenever there isn't *exactly* maxRowsPerBatch records in
    // this partition.
    if (statement != null) {
      statement.addBatch();
      statement.executeBatch();
      statement.close();
    }
  }

  private static PreparedStatement generatePreparedStatement(
      SortedMap<String, String> columns,
      int numRecords,
      Object tableName,
      Connection connection
  ) throws SQLException {
    String valuePlaceholder = String.format("(%s)", Joiner.on(", ").join(columns.values()));
    String valuePlaceholders = StringUtils.repeat(valuePlaceholder, ", ", numRecords);
    String query = String.format(
        "INSERT INTO %s (%s) VALUES %s",
        tableName,
        // keySet and values will both return the same ordering, due to using a SortedMap
        Joiner.on(", ").join(columns.keySet()),
        valuePlaceholders
    );

    return connection.prepareStatement(query);
  }

  private SortedMap<String, String> getFilteredColumnsToParameters(Map<String, String> parameters, Record record) {
    SortedMap<String, String> filtered = new TreeMap<>();
    for (Map.Entry<String, String> entry : getColumnsToFields().entrySet()) {
      String columnName = entry.getKey();
      String fieldPath = entry.getValue();

      if (record.has(fieldPath)) {
        filtered.put(columnName, parameters.get(columnName));
      }
    }
    return filtered;
  }

  /**
   * Partitions a batch into partitions of the same set of fields.
   * Does not sort the records in each partition.
   *
   * @param batch input batch of records
   * @return Multi-valued map of records. Key is a hash of the columns present.
   */
  private Multimap<Long, Record> partitionBatch(Collection<Record> batch) {
    Multimap<Long, Record> partitions = ArrayListMultimap.create();
    for (Record record : batch) {
      Long columnHash = getColumnHash(record).asLong();
      partitions.put(columnHash, record);
    }
    return partitions;
  }

  /**
   * Generates a hash for the fields present in a record and their mappings.
   * A specific implementation of the hash function is not guaranteed.
   *
   * @param record The record to generate a hash for.
   * @return A Guava HashCode of the fields.
   */
  private HashCode getColumnHash(Record record) {
    Map<String, String> parameters = getColumnsToParameters();
    SortedMap<String, String> columnsToParameters = getFilteredColumnsToParameters(parameters, record);
    return columnHashFunction.newHasher()
        .putObject(columnsToParameters, stringMapFunnel)
        .hash();
  }

  /**
   * This is an error that is not due to bad input record and should throw a StageException
   * once we format the error.
   *
   * @param e SQLException
   * @throws StageException
   */
  private static void handleSqlException(SQLException e) throws StageException {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.error(formattedError);
    LOG.debug(formattedError, e);
    throw new StageException(Errors.JDBCDEST_14, formattedError);
  }
}
