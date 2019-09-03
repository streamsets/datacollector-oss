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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class JdbcMySqlBinLogRecordReader extends JdbcRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcMySqlBinLogRecordReader.class);

  private static final String DATA_FIELD = "/Data";
  private static final String OLD_DATA_FIELD = "/OldData";

  /**
   * Records from MySQL BinLog origin have a bit unique structure.
   *
   * Records for Insert and update operations have a field path "/Data", which is a map
   * of all column names as key and values stored in DB as value.
   *
   * Record for Delete operation don't have /Data field. Instead it has a filed path "/OldData",
   * which store the original data in table. So need to look into /OldData when operation is delete.
   *
   * We expect user to configure /Data as a filed path in column-filed mapping configuration.
   * @param record
   * @param op
   * @param parameters
   * @param columnsToFields
   * @return
   */
  @Override
  public SortedMap<String, String> getColumnsToParameters(
      final Record record, int op,
      Map<String, String> parameters,
      Map<String, String> columnsToFields)
  {
    SortedMap<String, String> columnsToParameters = new TreeMap<>();
    for (Map.Entry<String, String> entry : columnsToFields.entrySet()) {
      String columnName = entry.getKey();
      String fieldPath = entry.getValue();
      if(op == OperationType.DELETE_CODE){
        fieldPath = fieldPath.replace(DATA_FIELD, OLD_DATA_FIELD);
      }

      if (record.has(fieldPath)) {
        columnsToParameters.put(columnName, parameters.get(columnName));
      }
    }
    return columnsToParameters;
  }

  /**
   * Return a fieldpath for the corresponding column name. The columnToField Map
   * contains column-to-field mapping, so simply get fieldpath for the column name
   * if operation is INSERT or UPDATE.
   * We replace /Data to /OldData for delete operation because that's where records
   * store primary key info.
   * @param columnName column name
   * @param columnsToField mapping of column name to field path
   * @param op  Not used here but is used in subclass.
   * @return
   */
  @Override
  public String getFieldPath(String columnName, Map<String, String> columnsToField, int op) {
    if (op == OperationType.DELETE_CODE){
      String fieldPath = columnsToField.get(columnName);
      if (fieldPath == null){
        LOG.error("Column name {} is not defined in column-filed mapping", columnName);
        return null;
      }
      return fieldPath.replace("/Data", "/OldData");
    }
    // For insert and update, ok to use the field name set by column-field mapping
    return columnsToField.get(columnName);
  }
}
