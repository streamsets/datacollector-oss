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
package com.streamsets.pipeline.stage.destination.kudu;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.lib.kudu.Errors;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

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

  public void convert(Record record, PartialRow row, int operation) throws OnRecordErrorException {
    for (Map.Entry<String, String> entry : fieldsToColumns.entrySet()) {
      String fieldName = entry.getKey(); // field name in record
      String column = entry.getValue();  // column name in Kudu table
      // For delete, we only need to fill primary key column name & value in PartialRow
      if (operation == KuduOperationType.DELETE.code){
        for(ColumnSchema col : schema.getPrimaryKeyColumns()) {
          if (col.getName().equals(column))
            recordToRow(record, row, fieldName, column, operation);
        }
      } else {
        // For other operations, we need to know the operation
        // to correctly fill the record.
        recordToRow(record, row, fieldName, column, operation);
      }
    }
  }

  private void recordToRow(Record record, PartialRow row, String fieldName, String column, int operation) throws OnRecordErrorException {
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
              if (columnSchema.getType() == Type.UNIXTIME_MICROS) {
                // Convert millisecond to microsecond
                row.addLong(column, field.getValueAsLong() * 1000);
              } else {
                row.addLong(column, field.getValueAsLong());
              }
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
        } catch (IllegalArgumentException e) {
          throw new OnRecordErrorException(record, Errors.KUDU_09, fieldName, type.name(), e.toString(), e);
        }
      }
    } else {
      // SDC-5816.  do not null out columns in UPDATE or UPSERT mode.
      // if the columns are not specified - they should not be changed.
      if(operation == KuduOperationType.INSERT.code) {
        if (!columnSchema.isNullable()) {
          throw new OnRecordErrorException(record, Errors.KUDU_06, column, fieldName);
        }
        row.setNull(column);
      }
    }
  }

}
