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
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriter;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcType;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;

public class JdbcSchemaReader {
  // JDBC Metadata Columns
  @VisibleForTesting
  public static final int COLUMN_NAME = 4;
  @VisibleForTesting
  public static final int DATA_TYPE = 5;
  @VisibleForTesting
  public static final int COLUMN_SIZE = 7;
  @VisibleForTesting
  public static final int DECIMAL_DIGITS = 9;

  private final HikariDataSource dataSource;
  private final JdbcSchemaWriter schemaWriter;

  public JdbcSchemaReader(HikariDataSource dataSource, JdbcSchemaWriter schemaWriter) {
    this.dataSource = dataSource;
    this.schemaWriter = schemaWriter;
  }

  public LinkedHashMap<String,JdbcTypeInfo> getTableSchema(String schema, String tableName) throws
      SQLException {
    LinkedHashMap<String, JdbcTypeInfo> columns = new LinkedHashMap<>();

    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet metaDataTables = metaData.getTables(null, schema, tableName, new String[]{"TABLE"});) {
        if (!metaDataTables.next()) {
          return columns;
        }

        ResultSet metaDataColumns = metaData.getColumns(null, schema, tableName, null);
        while (metaDataColumns.next()) {
          JdbcType jdbcType = JdbcType.valueOf(metaDataColumns.getInt(DATA_TYPE));
          columns.put(metaDataColumns.getString(COLUMN_NAME),
              jdbcType.getSupport().createTypeInfo(jdbcType, schemaWriter,
                  metaDataColumns.getInt(COLUMN_SIZE),
                  metaDataColumns.getInt(DECIMAL_DIGITS)));
        }
      }
    }

    return columns;
  }
}
