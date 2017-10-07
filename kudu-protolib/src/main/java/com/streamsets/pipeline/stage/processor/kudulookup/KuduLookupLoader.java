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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Meter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.lib.kudu.Errors;
import com.streamsets.pipeline.stage.lib.kudu.KuduUtils;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

public class KuduLookupLoader extends CacheLoader<KuduLookupKey, List<Map<String, Field>>> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduLookupLoader.class);

  private final AsyncKuduClient kuduClient;
  private final Meter selectMeter;
  private final Timer selectTimer;
  // For key columns
  private final List<String> keyColumns;
  private final Map<String, String> columnToField;
  // For output columns
  private final List<String> projectColumns = new ArrayList<>();
  private final Map<String, String> outputColumnToField = new HashMap<>();
  private final Map<String, String> outputDefault = new HashMap<>();

  private final LoadingCache<String, KuduTable> tableCache;

  private final boolean ignoreMissing;

  public KuduLookupLoader(Stage.Context context,
                          AsyncKuduClient kuduClient,
                          List<String> keyColumns,
                          Map<String, String> columnToField,
                          KuduLookupConfig conf
  ) {
    this.selectMeter = context.createMeter("Select Queries");
    this.selectTimer = context.createTimer("Select Queries");
    this.kuduClient = kuduClient;
    this.keyColumns = keyColumns;
    this.columnToField = columnToField;
    this.ignoreMissing = conf.ignoreMissing;

    // output fields
    for (KuduOutputColumnMapping columnConfig : conf.outputColumnMapping) {
      String columnName = conf.caseSensitive ? columnConfig.columnName : columnConfig.columnName.toLowerCase();
      projectColumns.add(columnName);
      outputColumnToField.put(columnName, columnConfig.field);
      outputDefault.put(columnName, columnConfig.defaultValue);
    }
    // Build table cache.
    CacheBuilder cacheBuilder;
    if (conf.enableTableCache) {
      cacheBuilder = CacheBuilder.newBuilder().maximumSize(0);
    } else {
      cacheBuilder = CacheBuilder.newBuilder().maximumSize(conf.cacheSize);
    }

    tableCache = cacheBuilder.build(new CacheLoader<String, KuduTable>() {
      @Override
      public KuduTable load(String tableName) throws Exception {
        return kuduClient.openTable(tableName).join();
      }
    });
  }

  @Override
  public List<Map<String, Field>> load(KuduLookupKey key) throws Exception {
    List<Map<String, Field>> lookupItems = new ArrayList<>();
    AsyncKuduScanner scanner = null;
    Timer.Context t = selectTimer.time();

    KuduTable kuduTable = null;
    try {
      kuduTable = tableCache.get(key.tableName);
    } catch (ExecutionException ex) {
      throw new OnRecordErrorException(Errors.KUDU_03, ex.getMessage(), ex);
    }

    // Scanner is not reusable. Need to build per record.
    AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable)
        .setProjectedColumnNames(projectColumns);
    List<String> kColumns = new ArrayList<>(keyColumns);
    try {
      // Set primary keys to scanner
      Schema schema = kuduTable.getSchema();
      for (ColumnSchema keySchema : schema.getPrimaryKeyColumns()) {
        if (!kColumns.contains(keySchema.getName())){
          // Primary key is not configured in Key Column Mapping. Worth stopping pipeline.
          throw new StageException(Errors.KUDU_34, keySchema.getName());
        }
        String keyColumnName = keySchema.getName();
        addPredicate(key.columns.get(keyColumnName), scannerBuilder, kuduTable, keySchema.getName());
        kColumns.remove(keyColumnName);
      }
      // Set non-primary key columns to scanner if specified in Key Column Mapping
      if (!kColumns.isEmpty()) {
        for (String nonPrimary : kColumns) {
          addPredicate(key.columns.get(nonPrimary), scannerBuilder, kuduTable, nonPrimary);
        }
      }
      try {
        scanner = scannerBuilder.build();
      } catch (IllegalArgumentException ex) {
        // Thrown here if mapping config has columns that don't exist in the table. Worth stopping pipeline
        throw new StageException(Errors.KUDU_02, ex);
      }

      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows().join();
        while (results.hasNext()) {
          RowResult result = results.next();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found row: {}", result.toStringLongFormat());
          }
          LinkedHashMap<String, Field> fields = new LinkedHashMap<>(outputColumnToField.size());
          for (Map.Entry<String, String> column : outputColumnToField.entrySet()) {
            Field field = null;
            Type type = null;
            String columnName = column.getKey();
            if (result.isNull(columnName)){
              // Apply default value or send to error
              if (ignoreMissing && !outputDefault.get(columnName).isEmpty()) {
                // Apply default value
                ColumnSchema columnSchema = schema.getColumn(columnName);
                field = Field.create(
                    KuduUtils.convertFromKuduType(columnSchema.getType()),
                    outputDefault.get(columnName)
                );
              } else {
                // Missing value for output, and default value is not configured
                throw new OnRecordErrorException(Errors.KUDU_35, columnName);
              }
            } else {
              type = result.getColumnType(column.getKey());
              field = KuduUtils.createField(result, columnName, type);
            }
            fields.put(column.getValue(), field);
          }
          lookupItems.add(fields);
        }
      }
    } catch (KuduException e) {
      // Exception executing query
      LOG.error(Errors.KUDU_03.getMessage(), e.toString(), e);
      throw new StageException(Errors.KUDU_03, e.toString(), e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      // If the timer wasn't stopped due to exception yet, stop it now
      if(t != null) {
        t.stop();
      }
      selectMeter.mark();
    }
    return lookupItems;
  }

  private void addPredicate(Field field, AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder, KuduTable kuduTable, String keyColumn)
      throws StageException
  {
    ColumnSchema schema;
    if (field == null) {
      throw new OnRecordErrorException(Errors.KUDU_32, keyColumn);
    }

    try {
      schema = kuduTable.getSchema().getColumn(keyColumn);
    } catch (IllegalArgumentException ex) {
      // Thrown if keyColumn doesn't exist in Kudu. Worth stopping pipeline
      throw new StageException(Errors.KUDU_03, Utils.format("Key column '{}' doesn't exist in Kudu table ", keyColumn), ex);
    }
    Type type = schema.getType();
    KuduPredicate predicate = null;
    String fieldName = columnToField.get(keyColumn);

    try {
      switch (type) {
        case STRING:
          if (field.getValueAsString().isEmpty()) {
            throw new OnRecordErrorException(Errors.KUDU_32, fieldName);
          }
          predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, field.getValueAsString());
          break;
        case INT8:
        case INT16:
        case INT32:
        case INT64:
          // API takes long type for all int
          predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, field.getValueAsLong());
          break;
        case BOOL:
          predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, field.getValueAsBoolean());
          break;
        case BINARY:
          predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, field.getValueAsByteArray());
          break;
        case UNIXTIME_MICROS:
          predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, field.getValueAsDatetime().getTime());
          break;
        default:
          throw new StageException(Errors.KUDU_33, type.getName());
      }
    } catch (IllegalArgumentException ex){
      throw new OnRecordErrorException(Errors.KUDU_09, fieldName, field.toString(), ex);
    }
    scannerBuilder.addPredicate(predicate);
  }
}
