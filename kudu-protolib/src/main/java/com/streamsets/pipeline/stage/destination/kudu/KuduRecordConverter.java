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

package com.streamsets.pipeline.stage.destination.kudu;


import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.client.PartialRow;

import java.util.Map;

public class KuduRecordConverter {

  private final Map<String, Field.Type> columnsToFieldTypes;
  private final Map<String, String> fieldsToColumns;
  private final Schema schema;

  public KuduRecordConverter(Map<String, Field.Type> columnsToFieldTypes, Map<String, String> fieldsToColumns,
                             Schema schema) {
    this.columnsToFieldTypes = ImmutableMap.copyOf(columnsToFieldTypes);
    this.fieldsToColumns = ImmutableMap.copyOf(fieldsToColumns);
    this.schema = schema;
  }

  public void convert(Record record, PartialRow row) throws OnRecordErrorException {
    for (Map.Entry<String, String> entry : fieldsToColumns.entrySet()) {
      String fieldName = entry.getKey();
      String column = entry.getValue();
      Field.Type type = columnsToFieldTypes.get(column);
      ColumnSchema columnSchema = schema.getColumn(column);
      if (record.has(fieldName)) {
        Field field = record.get(fieldName);
        if (field.getValue() == null) {
          if (!columnSchema.isNullable()) {
            throw new OnRecordErrorException(record, Errors.KUDU_06, column, fieldName);
          }
          row.setNull(column);
        } else {
          try {
            switch (type) {
              case BOOLEAN:
                row.addBoolean(column, field.getValueAsBoolean());
                break;
              case BYTE:
                row.addByte(column, field.getValueAsByte());
                break;
              case SHORT:
                row.addShort(column, field.getValueAsShort());
                break;
              case INTEGER:
                row.addInt(column, field.getValueAsInteger());
                break;
              case LONG:
                row.addLong(column, field.getValueAsLong());
                break;
              case FLOAT:
                row.addFloat(column, field.getValueAsFloat());
                break;
              case DOUBLE:
                row.addDouble(column, field.getValueAsDouble());
                break;
              case STRING:
                row.addString(column, field.getValueAsString());
                break;
              case BYTE_ARRAY:
                row.addBinary(column, field.getValueAsByteArray());
                break;
              default:
                throw new OnRecordErrorException(record, Errors.KUDU_04, fieldName, type.name());
            }
          } catch (NumberFormatException nfe) {
            throw new OnRecordErrorException(record, Errors.KUDU_09, fieldName, type.name(), nfe.toString(), nfe);
          }
        }
      } else {
        if (!columnSchema.isNullable()) {
          throw new OnRecordErrorException(record, Errors.KUDU_06, column, fieldName);
        }
        row.setNull(column);
      }
    }
  }
}
