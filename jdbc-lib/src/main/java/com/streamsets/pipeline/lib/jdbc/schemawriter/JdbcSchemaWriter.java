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
package com.streamsets.pipeline.lib.jdbc.schemawriter;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcType;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;

import java.util.LinkedHashMap;

/**
 * Database specific operations
 */
public interface JdbcSchemaWriter {
  /**
   * Creates a new database table
   *
   * @param schema Database schema name
   * @param tableName Table to be created
   * @param recordStructure Mapping of column names to JDBC types
   *
   * @throws JdbcStageCheckedException Wraps SQLExceptions etc
   */
  void createTable(String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> recordStructure) throws
      JdbcStageCheckedException;

  /**
   * Alters an existing database table
   *
   * @param schema Database schema name
   * @param tableName Table to be altered
   * @param recordStructure Mapping of new column names to JDBC types
   *
   * @throws JdbcStageCheckedException Wraps SQLExceptions etc
   */
  void alterTable(String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> recordStructure) throws
      JdbcStageCheckedException;

  /**
   * Get the column data type, for example "double precision", or "decimal(10, 20)"
   *
   * @param jdbcType JDBC type
   *
   * @return String representation of the column's data type
   */
  String getColumnTypeName(JdbcType jdbcType);

  /**
   * Validate that the precision and scale fall within valid bounds for
   * the database, and that the field value fits within the precision
   * and scale.
   *
   * @param fieldName Name of the field - used in error reporting
   * @param field Field to be validated
   * @param precision Precision to be validated
   * @param scale Scale to be validated
   *
   * @throws JdbcStageCheckedException
   */
  void validateScaleAndPrecision(String fieldName, Field field, int precision, int scale) throws
      JdbcStageCheckedException;

  /**
   * @return Default schema name, or null if there isn't one
   */
  String getDefaultSchema();
}
