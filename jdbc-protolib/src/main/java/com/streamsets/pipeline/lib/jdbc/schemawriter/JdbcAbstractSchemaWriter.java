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
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class JdbcAbstractSchemaWriter implements JdbcSchemaWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcAbstractSchemaWriter.class);
  private static final String PRECISION = "precision";
  private static final String SCALE = "scale";

  protected static final String CREATE_TABLE = "CREATE TABLE";
  protected static final String ALTER_TABLE = "ALTER TABLE";

  private final HikariDataSource dataSource;

  JdbcAbstractSchemaWriter(HikariDataSource dataSource) {
    this.dataSource = dataSource;
  }

  private void executeStatement(String sqlString) throws JdbcStageCheckedException {
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement()) {
      LOG.debug("Executing statement '{}'", sqlString);
      statement.execute(sqlString);
      connection.commit();
    } catch (SQLException e) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_02, sqlString, e.getMessage(), e);
    }
  }

  @Override
  public void createTable(String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> recordStructure
  ) throws JdbcStageCheckedException {
    executeStatement(makeCreateTableSqlString(schema, tableName, recordStructure));
  }

  @Override
  public void alterTable(String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> columnDiff
  ) throws JdbcStageCheckedException {
    executeStatement(makeAlterTableSqlString(schema, tableName, columnDiff));
  }

  protected String makeCreateTableSqlString(
      String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> recordStructure
  ) {
    StringBuilder sqlString = new StringBuilder(CREATE_TABLE + " ");
    String tableSchema = (schema == null) ? getDefaultSchema() : schema;
    if (tableSchema != null) {
      sqlString.append('"');
      sqlString.append(tableSchema);
      sqlString.append('"');
      sqlString.append(".");
    }
    sqlString.append('"');
    sqlString.append(tableName);
    sqlString.append('"');
    sqlString.append("(\n");
    boolean first = true;
    for (Map.Entry<String, JdbcTypeInfo> entry : recordStructure.entrySet()) {
      if (first) {
        first = false;
      } else {
        sqlString.append(",\n");
      }
      sqlString
          .append('"')
          .append(entry.getKey())
          .append('"')
          .append(" ")
          .append(entry.getValue().toString());
    }
    sqlString.append("\n);");

    return sqlString.toString();
  }

  protected String makeAlterTableSqlString(
      String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> columnDiff
  ) {
    StringBuilder sqlString = new StringBuilder(ALTER_TABLE + " ");
    String tableSchema = (schema == null) ? getDefaultSchema() : schema;
    if (tableSchema != null) {
      sqlString.append('"');
      sqlString.append(tableSchema);
      sqlString.append('"');
      sqlString.append(".");
    }
    sqlString.append('"');
    sqlString.append(tableName);
    sqlString.append('"');
    sqlString.append("\n");
    boolean first = true;
    for (Map.Entry<String, JdbcTypeInfo> entry : columnDiff.entrySet()) {
      if (first) {
        first = false;
      } else {
        sqlString.append(",\n");
      }
      sqlString
          .append("ADD COLUMN")
          .append(" ")
          .append('"')
          .append(entry.getKey())
          .append('"')
          .append(" ")
          .append(entry.getValue().toString());
    }
    sqlString.append(";");

    return sqlString.toString();
  }

  public void validateScaleAndPrecision(String fieldName, Field field, int precision, int scale) throws JdbcStageCheckedException{
    // Validate calculated precision/scale
    if (precision > getMaxPrecision()) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_305, precision, PRECISION, fieldName, 1, getMaxPrecision());
    }
    if (scale > getMaxScale()) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_305, scale, SCALE, fieldName, 0, getMaxScale());
    }
    if (precision < 1) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_305, precision, PRECISION, fieldName, 1, getMaxPrecision());
    }
    if (scale < 0) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_305, scale, SCALE, fieldName, 0, getMaxScale());
    }
    if (scale > precision) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_306, scale, fieldName, precision);
    }

    // Validate that given decimal value is in the range of what was calculated
    BigDecimal value = field.getValueAsDecimal();
    if(value != null) {
      if (value.scale() > scale) {
        throw new JdbcStageCheckedException(JdbcErrors.JDBC_307, value, fieldName, SCALE, value.scale(), scale);
      }
      if (value.precision() > precision) {
        throw new JdbcStageCheckedException(JdbcErrors.JDBC_307, value, fieldName,
            PRECISION, value.precision(), precision);
      }
    }
  }

  protected abstract int getMaxScale();

  protected abstract int getMaxPrecision();

  @Override
  public String getDefaultSchema() { return null; }
}
