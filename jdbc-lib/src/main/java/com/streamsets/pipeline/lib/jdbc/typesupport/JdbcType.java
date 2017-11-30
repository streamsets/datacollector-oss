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
package com.streamsets.pipeline.lib.jdbc.typesupport;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;

import java.sql.Types;
import java.util.Map;

/**
 * Hive Types Supported.
 */
public enum JdbcType {
  BOOLEAN(new PrimitiveJdbcTypeSupport()),
  CHAR(new PrimitiveJdbcTypeSupport()),
  DATE(new PrimitiveJdbcTypeSupport()),
  TIMESTAMP(new PrimitiveJdbcTypeSupport()),
  TIME(new PrimitiveJdbcTypeSupport()),
  INTEGER(new PrimitiveJdbcTypeSupport()),
  BIGINT(new PrimitiveJdbcTypeSupport()),
  FLOAT(new PrimitiveJdbcTypeSupport()),
  DOUBLE(new PrimitiveJdbcTypeSupport()),
  DECIMAL(new DecimalJdbcTypeSupport()),
  BINARY(new PrimitiveJdbcTypeSupport()),
  VARCHAR(new PrimitiveJdbcTypeSupport())
  ;
  static Map<Integer, JdbcType> sqlToJdbcType = ImmutableMap.<Integer, JdbcType>builder()
      // TBD - fill this out
      .put(Types.BOOLEAN, BOOLEAN)
      .put(Types.BIT, BOOLEAN)
      .put(Types.CHAR, CHAR)
      .put(Types.DATE, DATE)
      .put(Types.TIMESTAMP, TIMESTAMP)
      .put(Types.TIME, TIME)
      .put(Types.INTEGER, INTEGER)
      .put(Types.BIGINT, BIGINT)
      .put(Types.FLOAT, FLOAT)
      .put(Types.DOUBLE, DOUBLE)
      .put(Types.NUMERIC, DECIMAL)
      .put(Types.DECIMAL, DECIMAL)
      .put(Types.BINARY, BINARY)
      .put(Types.VARCHAR, VARCHAR)
      .build();

  JdbcTypeSupport support;

  JdbcType(JdbcTypeSupport support) {
    this.support = support;
  }

  public JdbcTypeSupport getSupport() {
    return support;
  }
  /**
   * Return the corresponding {@link JdbcType} for the {@link Field.Type}
   * @param fieldType Record Field Type
   * @return {@link JdbcType}
   * @throws StageException if it is an unsupported {@link Field.Type} or null
   */
  public static JdbcType getJdbcTypeforFieldType(Field.Type fieldType) throws JdbcStageCheckedException {
    switch (fieldType) {
      case BOOLEAN: return JdbcType.BOOLEAN;
      case CHAR: return JdbcType.CHAR;
      case DATE: return JdbcType.DATE;
      case DATETIME: return JdbcType.TIMESTAMP;
      case TIME: return JdbcType.TIME;
      case INTEGER: return JdbcType.INTEGER;
      case LONG: return JdbcType.BIGINT;
      case FLOAT: return JdbcType.FLOAT;
      case DOUBLE: return JdbcType.DOUBLE;
      case DECIMAL: return JdbcType.DECIMAL;
      case STRING: return JdbcType.VARCHAR;
      case BYTE_ARRAY: return JdbcType.BINARY;
      default: throw new JdbcStageCheckedException(JdbcErrors.JDBC_302, fieldType);
    }
  }

  /**
   * Returns the corresponding {@link Field.Type} for {@link JdbcType}.
   * @param JdbcType {@link JdbcType}
   * @return {@link Field.Type}
   * @throws StageException if it is an unsupported {@link JdbcType} or null
   */
  public static Field.Type getFieldTypeForJdbcType(JdbcType JdbcType) throws JdbcStageCheckedException {
    switch (JdbcType) {
      case BOOLEAN: return Field.Type.BOOLEAN;
      case DATE: return Field.Type.DATE;
      case TIME: return Field.Type.TIME;
      case TIMESTAMP: return Field.Type.DATETIME;
      case INTEGER: return Field.Type.INTEGER;
      case BIGINT: return Field.Type.LONG;
      case FLOAT: return Field.Type.FLOAT;
      case DOUBLE: return Field.Type.DOUBLE;
      case DECIMAL: return Field.Type.DECIMAL;
      case BINARY: return Field.Type.BYTE_ARRAY;
      case VARCHAR: return Field.Type.STRING;
      default: throw new JdbcStageCheckedException(JdbcErrors.JDBC_302, JdbcType);
    }
  }

  /**
   * Returns the corresponding {@link JdbcType} for the string representation.<br>
   * @param JdbcTypeString case insensitive string representation of corresponding {@link JdbcType}
   * @return {@link JdbcType}
   */
  public static JdbcType getJdbcTypeFromString(String JdbcTypeString) {
    return JdbcType.valueOf(JdbcTypeString.toUpperCase());
  }

  public static JdbcType prefixMatch(String JdbcTypeString) throws JdbcStageCheckedException {
    for (JdbcType JdbcType : JdbcType.values()) {
      if (JdbcTypeString.toUpperCase().startsWith(JdbcType.name().toUpperCase())) {
        return JdbcType;
      }
    }
    throw new JdbcStageCheckedException(JdbcErrors.JDBC_301, "Invalid Hive Type Definition: {} " + JdbcTypeString);
  }

  public static JdbcType valueOf(int sqlType) {
    return sqlToJdbcType.get(sqlType);
  }
}
