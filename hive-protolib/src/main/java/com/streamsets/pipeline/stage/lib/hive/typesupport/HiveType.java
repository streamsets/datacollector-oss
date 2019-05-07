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
package com.streamsets.pipeline.stage.lib.hive.typesupport;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;

/**
 * Hive Types Supported.
 */
public enum HiveType {
  BOOLEAN(new PrimitiveHiveTypeSupport()),
  DATE(new PrimitiveHiveTypeSupport()),
  TIMESTAMP(new PrimitiveHiveTypeSupport()), // be careful as enum types order matters for prefixMatch method below
  INT(new PrimitiveHiveTypeSupport()),
  BIGINT(new PrimitiveHiveTypeSupport()),
  FLOAT(new PrimitiveHiveTypeSupport()),
  DOUBLE(new PrimitiveHiveTypeSupport()),
  DECIMAL(new DecimalHiveTypeSupport()),
  BINARY(new PrimitiveHiveTypeSupport()),
  STRING(new PrimitiveHiveTypeSupport())
  ;

  HiveTypeSupport support;

  HiveType(HiveTypeSupport support) {
    this.support = support;
  }

  public HiveTypeSupport getSupport() {
    return support;
  }
  /**
   * Return the corresponding {@link HiveType} for the {@link com.streamsets.pipeline.api.Field.Type}
   * @param fieldType Record Field Type
   * @return {@link HiveType}
   * @throws StageException if it is an unsupported {@link com.streamsets.pipeline.api.Field.Type} or null
   */
  public static HiveType getHiveTypeforFieldType(Field.Type fieldType) throws HiveStageCheckedException {
    switch (fieldType) {
      case BOOLEAN: return HiveType.BOOLEAN;
      case DATETIME: return HiveType.TIMESTAMP;
      case DATE: return HiveType.DATE;
      case INTEGER: return HiveType.INT;
      case LONG: return HiveType.BIGINT;
      case FLOAT: return HiveType.FLOAT;
      case DOUBLE: return HiveType.DOUBLE;
      case DECIMAL: return HiveType.DECIMAL;
      case STRING: return HiveType.STRING;
      case BYTE_ARRAY: return HiveType.BINARY;
      default: throw new HiveStageCheckedException(Errors.HIVE_19, fieldType);
    }
  }

  /**
   * Returns the corresponding {@link Field.Type} for {@link HiveType}.
   * No support for timestamp type as impala will consider timestamp as string during table creation/alter.
   * @param hiveType {@link HiveType}
   * @return {@link Field.Type}
   * @throws StageException if it is an unsupported {@link HiveType} or null
   */
  public static Field.Type getFieldTypeForHiveType(HiveType hiveType) throws HiveStageCheckedException {
    switch (hiveType) {
      case BOOLEAN: return Field.Type.BOOLEAN;
      case DATE: return Field.Type.DATE;
      case TIMESTAMP: return Field.Type.DATETIME;
      case INT: return Field.Type.INTEGER;
      case BIGINT: return Field.Type.LONG;
      case FLOAT: return Field.Type.FLOAT;
      case DOUBLE: return Field.Type.DOUBLE;
      case DECIMAL: return Field.Type.DECIMAL;
      case BINARY: return Field.Type.BYTE_ARRAY;
      //Suffice to say don't need timestamp
      //(Won't be a problem if we are the ones creating the table)
      case STRING: return Field.Type.STRING;
      default: throw new HiveStageCheckedException(Errors.HIVE_19, hiveType);
    }
  }

  /**
   * Returns the corresponding {@link HiveType} for the string representation.<br>
   * @param hiveTypeString case insensitive string representation of corresponding {@link HiveType}
   * @return {@link HiveType}
   */
  public static HiveType getHiveTypeFromString(String hiveTypeString) {
    return HiveType.valueOf(hiveTypeString.toUpperCase());
  }

  public static HiveType prefixMatch(String hiveTypeString) throws HiveStageCheckedException {
    for (HiveType hiveType : HiveType.values()) {
      if (hiveTypeString.toUpperCase().startsWith(hiveType.name().toUpperCase())) {
        return hiveType;
      }
    }
    throw new HiveStageCheckedException(Errors.HIVE_01, "Invalid Hive Type Definition: {} " + hiveTypeString);
  }
}
