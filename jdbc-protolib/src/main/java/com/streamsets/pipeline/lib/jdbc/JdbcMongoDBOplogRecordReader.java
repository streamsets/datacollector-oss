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

public class JdbcMongoDBOplogRecordReader extends JdbcRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcMongoDBOplogRecordReader.class);

  private static final String ID_FIELD = "_id";
  private static final String OP_FIELD = "o";
  private static final String OP2_FIELD = "o2";
  private static final String SET_FIELD = "\\$set";

  /**
   * Records from MongoDB Oplog origin have a bit unique structure.
   *
   * Records for Insert have all data in the field "o", which is a map and contains all data for columns
   * Records for Delete have objectId in the field "o", which is a map and contains only objectId.
   * Records for Update have a field "o2", which is a map and contains only objectId,
   * and a field "o" where it stores data for updating columns and values.
   *
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
      String fieldPath = getFieldPath(columnName, columnsToFields, op);
      if (record.has(fieldPath)) {
        columnsToParameters.put(columnName, parameters.get(columnName));
      } else {
        LOG.trace("Record is missing a field for column {} for the operation code {}", columnName, op);
      }
    }
    return columnsToParameters;
  }

  /**
   * Return a fieldpath for the corresponding column name. The columnToField Map
   * contains column-to-field mapping, so simply get fieldpath for the column name
   * if operation is INSERT or DELETE.
   * We replace /o with /o2 to get _id field, and change /o to /o/$set to get
   * updating column & value fields.
   * @param columnName column name
   * @param columnsToField mapping of column name to field path
   * @param op  Not used here but is used in subclass.
   * @return
   */
  @Override
  public String getFieldPath(String columnName, Map<String, String> columnsToField, int op) {
    if (op == OperationType.UPDATE_CODE){
      String fieldPath = columnsToField.get(columnName);
      if (fieldPath == null){
        LOG.error("Column name {} is not defined in column-filed mapping", columnName);
        return null;
      }

      if (fieldPath.contains(ID_FIELD)) {
        // _id is stored in "/o2/column_name for update records. Need to change the fieldpath"
        return fieldPath.replace(OP_FIELD, OP2_FIELD);
      } else {
        // column and values are stored in "/o/$set/column_name". Need to change the fieldpath
        return fieldPath.replaceFirst(OP_FIELD, String.format("%s/%s", OP_FIELD, SET_FIELD));
      }

    }
    return columnsToField.get(columnName);
  }
}
