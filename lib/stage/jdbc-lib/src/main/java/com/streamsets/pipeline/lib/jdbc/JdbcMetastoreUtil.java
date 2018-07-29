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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriter;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcType;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class JdbcMetastoreUtil {
  private JdbcMetastoreUtil() {}

  /**
   *  Evaluate precision or scale in context of record and given field path.
   */
  private static int resolveScaleOrPrecisionExpression(
      String type,
      Field field,
      String attributeName,
      String fieldPath
  ) throws JdbcStageCheckedException {
    String stringValue = field.getAttribute(attributeName);

    try {
      return Integer.parseInt(stringValue);
    } catch (NumberFormatException e) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_304, type, fieldPath, attributeName, stringValue, e);
    }
  }

  public static LinkedHashMap<String, JdbcTypeInfo> convertRecordToJdbcType(
      Record record,
      String precisionAttribute,
      String scaleAttribute,
      JdbcSchemaWriter schemaWriter
  ) throws OnRecordErrorException, JdbcStageCheckedException {
    if (!record.get().getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
      throw new OnRecordErrorException(record,
          JdbcErrors.JDBC_300,
          record.getHeader().getSourceId(),
          record.get().getType().toString()
      );
    }
    LinkedHashMap<String, JdbcTypeInfo> columns = new LinkedHashMap<>();
    Map<String, Field> list = record.get().getValueAsMap();
    for(Map.Entry<String,Field> pair:  list.entrySet()) {
      if (StringUtils.isEmpty(pair.getKey())) {
        throw new OnRecordErrorException(record, JdbcErrors.JDBC_301, "Field name is empty");
      }
      String fieldPath = pair.getKey();
      Field currField = pair.getValue();

      // Some types requires special checks or alterations
      JdbcType jdbcType = JdbcType.getJdbcTypeforFieldType(currField.getType());
      JdbcTypeInfo jdbcTypeInfo;
      if (jdbcType == JdbcType.DECIMAL) {
        int precision = resolveScaleOrPrecisionExpression("precision", currField, precisionAttribute, fieldPath);
        int scale = resolveScaleOrPrecisionExpression("scale", currField, scaleAttribute, fieldPath);
        schemaWriter.validateScaleAndPrecision(pair.getKey(), currField, precision, scale);
        jdbcTypeInfo = jdbcType.getSupport().generateJdbcTypeInfoFromRecordField(currField, schemaWriter, precision, scale);
      } else {
        jdbcTypeInfo = jdbcType.getSupport().generateJdbcTypeInfoFromRecordField(currField, schemaWriter);
      }

      columns.put(pair.getKey().toLowerCase(), jdbcTypeInfo);
    }
    return columns;
  }

  public static LinkedHashMap<String, JdbcTypeInfo> getDiff(
      LinkedHashMap<String, JdbcTypeInfo> oldState,
      LinkedHashMap<String, JdbcTypeInfo> newState
  ) throws JdbcStageCheckedException{
    LinkedHashMap<String, JdbcTypeInfo> columnDiff = new LinkedHashMap<>();
    for (Map.Entry<String, JdbcTypeInfo> entry : newState.entrySet()) {
      String columnName = entry.getKey();
      JdbcTypeInfo columnTypeInfo = entry.getValue();
      if (!oldState.containsKey(columnName)) {
        columnDiff.put(columnName, columnTypeInfo);
      } else if (!oldState.get(columnName).equals(columnTypeInfo)) {
        throw new JdbcStageCheckedException(
            JdbcErrors.JDBC_303,
            columnName,
            oldState.get(columnName).toString(),
            columnTypeInfo.toString()
        );
      }
    }
    return columnDiff;
  }
}
