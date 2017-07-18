/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.kudu.Errors;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KuduLookupLoader extends CacheLoader<Record, List<Map<String, Field>>> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduLookupLoader.class);

  private final KuduClient kuduClient;
  private final KuduTable kuduTable;
  private final Meter selectMeter;
  private final Timer selectTimer;
  private final List<String> projectColumns;
  private final Map<String, String> columnToField;
  private final Map<String, String> fieldToColumn;

  public KuduLookupLoader(Stage.Context context,
                          KuduClient kuduClient,
                          KuduTable kuduTable,
                          List<String> projectColumns,
                          Map<String, String> columnToField,
                          Map<String, String> fieldToColumn,
                          KuduLookupConfig config) {
    this.selectMeter = context.createMeter("Select Queries");
    this.selectTimer = context.createTimer("Select Queries");
    this.kuduClient = kuduClient;
    this.kuduTable = kuduTable;
    this.projectColumns = projectColumns;
    this.columnToField = columnToField;
    this.fieldToColumn = fieldToColumn;
  }

  @Override
  public List<Map<String, Field>> load(Record record) throws Exception {
    LOG.debug("Looking up values for:  {}", record);
    List<Map<String, Field>> lookupItems = new ArrayList<>();
    KuduScanner scanner = null;
    Timer.Context t = selectTimer.time();
    try {
      KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable)
          .setProjectedColumnNames(projectColumns);
      for (ColumnSchema keySchema : kuduTable.getSchema().getPrimaryKeyColumns()) {
        String predicateColumn = keySchema.getName();
        ColumnSchema schema = kuduTable.getSchema().getColumn(predicateColumn);
        Type type = schema.getType();
        String fieldName = columnToField.get(predicateColumn);
        if (type == Type.STRING) {
          String value = record.get(fieldName).getValueAsString();
          if (value == null) {
            throw new OnRecordErrorException(record, Errors.KUDU_32, fieldName);
          } else {
            scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, value));
          }
        } else if (type == Type.INT8 || type == Type.INT16 || type == Type.INT32 || type == Type.INT64) {
          Long value = record.get(fieldName).getValueAsLong();
          if (value == null) {
            throw new OnRecordErrorException(record, Errors.KUDU_32, fieldName);
          } else {
            scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.EQUAL, value));
          }
        } else {
          throw new StageException(Errors.KUDU_33, type.getName());
        }
      }
      scanner = scannerBuilder.build();
      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult result = results.next();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found row: {}", result.toStringLongFormat());
          }
          LinkedHashMap<String, Field> fields = new LinkedHashMap<>(projectColumns.size());
          for (String column : projectColumns) {
            Type type = result.getColumnType(column);
            Field field = null;
            switch (type) {
              case INT8:
                field = Field.create(Field.Type.BYTE, result.getByte(column));
                break;
              case INT16:
                field = Field.create(Field.Type.SHORT, result.getShort(column));
                break;
              case INT32:
                field = Field.create(Field.Type.INTEGER, result.getInt(column));
                break;
              case INT64:
                field = Field.create(Field.Type.LONG, result.getLong(column));
                break;
              case BINARY:
                field = Field.create(Field.Type.BYTE_ARRAY, result.getBinary(column));
                break;
              case STRING:
                field = Field.create(Field.Type.STRING, result.getString(column));
                break;
              case BOOL:
                field = Field.create(Field.Type.BOOLEAN, result.getBoolean(column));
                break;
              case FLOAT:
                field = Field.create(Field.Type.FLOAT, result.getFloat(column));
                break;
              case DOUBLE:
                field = Field.create(Field.Type.DOUBLE, result.getDouble(column));
                break;
              default:
                throw new StageException(Errors.KUDU_10, column, type.getName());
            }
            String fieldName;
            if (columnToField.containsKey(column)) {
              fieldName = columnToField.get(column);
            } else {
              fieldName = column;
            }
            fields.put(fieldName, field);
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
}
