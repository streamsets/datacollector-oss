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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.jdbc.Errors;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcFieldMappingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @throws StageException
   */
  public JdbcMultiRowRecordWriter(
      String connectionString,
      DataSource dataSource,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldMappingConfig> customMappings
  ) throws StageException {
    super(connectionString, dataSource, tableName, rollbackOnError, customMappings);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<OnRecordErrorException> writeBatch(Batch batch) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();

      MultiRowInsertMap statementsForBatch = new MultiRowInsertMap(connection, getTableName());

      // Since we are doing multi-row inserts we have to partition the batch into groups of the same
      // set of fields. We'll also sort each partition for optimal inserts into column stores.

      Multimap<Long, Record> partitions = partitionBatch(batch);

      for (Long partitionKey : partitions.keySet()) {
        Collection<Record> partition = partitions.get(partitionKey);
        // Fetch the base insert query for this partition.
        SortedMap<String, String> columnsToParameters = getFilteredColumnsToParameters(
            getColumnsToParameters(), partition.iterator().next()
        );
        PreparedStatement statement = statementsForBatch.getInsertFor(
            // Multimap collections are never empty so this is a safe operation
            columnsToParameters,
            partition.size()
        );
        // Add each record's values to the query.
        int i = 1;
        for (Record record : partition) {
          for (String column : columnsToParameters.keySet()) {

            Field field = record.get(getColumnsToFields().get(column));
            Field.Type fieldType = field.getType();
            Object value = field.getValue();

            switch (fieldType) {
              case LIST:
                List<Object> unpackedList = new ArrayList<>();
                for (Field item : (List<Field>) value) {
                  unpackedList.add(item.getValue());
                }
                Array array = connection.createArrayOf(getSQLTypeName(fieldType), unpackedList.toArray());
                statement.setArray(i, array);
                break;
              case DATE:
              case DATETIME:
                // Java Date types are not accepted by JDBC drivers, so we need to convert ot java.sql.Date
                java.util.Date date = field.getValueAsDate();
                statement.setObject(i, new java.sql.Date(date.getTime()));
                break;
              default:
                statement.setObject(i, value);
                break;
            }
            ++i;
          }
        }
        // Done populating the INSERT for this partition
        statement.addBatch();
      }

      for (PreparedStatement statement : statementsForBatch.getStatements()) {
        try {
          statement.executeBatch();
        } catch (SQLException e) {
          if (getRollbackOnError()) {
            connection.rollback();
          }
          handleBatchUpdateException(batch, e, errorRecords);
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
  private Multimap<Long, Record> partitionBatch(Batch batch) {
    Multimap<Long, Record> partitions = ArrayListMultimap.create();
    Iterator<Record> recordIterator = batch.getRecords();
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
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
  private void handleSqlException(SQLException e) throws StageException {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.error(formattedError);
    LOG.debug(formattedError, e);
    throw new StageException(Errors.JDBCDEST_14, formattedError);
  }

  /**
   * <p>
   *   Some databases drivers allow us to figure out which record in a particular batch failed.
   * </p>
   * <p>
   *   In the case that we have a list of update counts, we can mark just the record as erroneous.
   *   Otherwise we must send the entire batch to error.
   * </p>
   *
   * @param batch Current batch
   * @param e BatchUpdateException
   * @param errorRecords List of error records for this batch
   */
  private void handleBatchUpdateException(
      Batch batch,
      SQLException e,
      List<OnRecordErrorException> errorRecords
  ) throws StageException {
    if (JdbcUtil.isDataError(getConnectionString(), e)) {
      String formattedError = JdbcUtil.formatSqlException(e);
      LOG.error(formattedError);
      LOG.debug(formattedError, e);

      Iterator<Record> failedRecords = batch.getRecords();
      if (!getRollbackOnError() && e instanceof BatchUpdateException &&
          ((BatchUpdateException) e).getUpdateCounts().length > 0) {
        BatchUpdateException bue = (BatchUpdateException) e;

        int i = 0;
        while (failedRecords.hasNext()) {
          Record record = failedRecords.next();
          if (i >= bue.getUpdateCounts().length ||
              bue.getUpdateCounts()[i] == PreparedStatement.EXECUTE_FAILED) {
            errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_14, formattedError));
          }
          i++;
        }
      } else {
        while (failedRecords.hasNext()) {
          errorRecords.add(new OnRecordErrorException(failedRecords.next(), Errors.JDBCDEST_14, formattedError));
        }
      }
    } else {
      handleSqlException(e);
    }
  }
}
