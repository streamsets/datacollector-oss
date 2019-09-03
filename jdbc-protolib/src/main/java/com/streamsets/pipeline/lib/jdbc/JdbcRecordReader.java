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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class JdbcRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcRecordReader.class);

  /**
   * Get the numeric operation code from record header. The default code is
   * used if the operation code is not found in the header.
   *
   * @param record the record to find the operation code
   * @param defaultOp the default operation
   * @param unsupportedAction the action to take for unsupported code
   * @param errorRecords the list to take error records
   * @return the numeric operation code or -1 for unsupported operation
   */
  @VisibleForTesting
  final int getOperationFromRecord(
      Record record,
      JDBCOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      List<OnRecordErrorException> errorRecords
  ) {
    return getOperationFromRecord(record, defaultOp.getCode(), unsupportedAction, errorRecords);
  }

  /**
   * Get the numeric operation code from record header. The default code is
   * used if the operation code is not found in the header. This can be
   * overwritten in inherited classes.
   *
   * @param record the record to find the operation code
   * @param defaultOpCode the default operation code
   * @param unsupportedAction the action to take for unsupported code
   * @param errorRecords the list to take error records
   * @return the numeric operation code or -1 for unsupported operation
   */
  public int getOperationFromRecord(
      Record record,
      int defaultOpCode,
      UnsupportedOperationAction unsupportedAction,
      List<OnRecordErrorException> errorRecords
  ) {
    String op = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
    int opCode = -1; // unsupported

    if (Strings.isNullOrEmpty(op)) {
      return defaultOpCode;
    }

    // Check if the operation code from header attribute is valid
    try {
      opCode = JDBCOperationType.convertToIntCode(op);
    } catch (NumberFormatException | UnsupportedOperationException ex) {
      LOG.debug(
          "Operation obtained from record is not supported. Handle by UnsupportedOperationAction {}. {}",
          unsupportedAction.getLabel(),
          ex
      );
      switch (unsupportedAction) {
        case SEND_TO_ERROR:
          LOG.debug("Sending record to error due to unsupported operation {}", op);
          errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_70, op));
          break;
        case USE_DEFAULT:
          opCode = defaultOpCode;
          break;
        case DISCARD:
        default: // unknown action
          LOG.debug("Discarding record with unsupported operation {}", op);
      }
    }
    return opCode;
  }

  /**
   * Iterate the columnsToField map and check if the record has all the necessary columns.
   * The returned SortedMap contains columnName-param mapping which is only contained in this record.
   * For example, if a table in destination DB has a column A but record doesn't have a field named A,
   * the column A is not included in the returning SortedMap.
   * @param record Record to check
   * @param op Operation code. Used mainly in subclass
   * @param parameters columnName-to-value mapping
   * @param columnsToFields columnName-to-fieldPath mapping
   * @return
   */
  @VisibleForTesting
   SortedMap<String, String> getColumnsToParameters(
      final Record record,
      int op,
      Map<String, String> parameters,
      Map<String, String> columnsToFields
  ) {
    return getColumnsToParameters(record, op, parameters, columnsToFields, true);
  }

  public <T extends Map<String, String>> T getColumnsToParameters(
      final Record record,
      int op,
      Map<String, String> parameters,
      Map<String, String> columnsToFields,
      boolean sortedMap
  ) {
    Map<String, String> filtered = null;
    if (sortedMap) {
      filtered = new TreeMap<>();
    } else {
      filtered = new LinkedHashMap<>();
    }
    for (Map.Entry<String, String> entry : columnsToFields.entrySet()) {
      String columnName = entry.getKey();
      String fieldPath = entry.getValue();

      if (record.has(fieldPath)) {
        filtered.put(columnName, parameters.get(columnName));
      } else {
        LOG.trace("Record is missing a field for column {} for the operation code {}", columnName, op);
      }
    }
    return (T) filtered;
  }

  /**
   * This function simply returns field path in record for the corresponding column name.
   * This is needed because records generated by CDC origins store data in different location
   * for different operation. Subclasses will override this function and implement special handling.
   * @param columnName column name
   * @param columnsToField mapping of column name to field path
   * @param op  Not used here but is used in subclass.
   * @return
   */
  public String getFieldPath(String columnName, Map<String, String> columnsToField, int op){
    return columnsToField.get(columnName);
  }
}
