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
package com.streamsets.pipeline.stage.origin.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.streamsets.pipeline.stage.origin.mysql.schema.Column;
import com.streamsets.pipeline.stage.origin.mysql.schema.DatabaseAndTable;
import com.streamsets.pipeline.stage.origin.mysql.schema.MysqlType;
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;
import com.streamsets.pipeline.stage.origin.mysql.schema.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlSchemaRepository {
  private static final String TABLE_SCHEMA_SQL =
      "SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
          "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

  private static final Logger LOG = LoggerFactory.getLogger(MysqlSchemaRepository.class);

  private final DataSource dataSource;

  public MysqlSchemaRepository(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  private final LoadingCache<DatabaseAndTable, Optional<? extends Table>> cache = CacheBuilder.newBuilder()
      .build(new CacheLoader<DatabaseAndTable, Optional<? extends Table>>() {
        @Override
        public Optional<? extends Table> load(DatabaseAndTable databaseAndTable) throws Exception {
          return loadTable(databaseAndTable);
        }
      });

  public Optional<? extends Table> getTable(DatabaseAndTable table) {
    try {
      return cache.get(table);
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new SchemaLoadException(e.getMessage(), e.getCause() != null ? e.getCause() : e);
    }
  }

  public void evictAll() {
    cache.invalidateAll();
  }

  public void evict(String database, String table) {
    cache.invalidate(new DatabaseAndTable(database, table));
  }

  private Optional<? extends Table> loadTable(DatabaseAndTable databaseAndTable) {
    try (Connection conn = dataSource.getConnection()) {
      try (PreparedStatement stmt = conn.prepareStatement(TABLE_SCHEMA_SQL)) {
        stmt.setString(1, databaseAndTable.getDatabase());
        stmt.setString(2, databaseAndTable.getTable());
        List<Column> columns = new ArrayList<>();
        try(ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String name = rs.getString(1);
            String type = rs.getString(2);
            columns.add(new Column(name, MysqlType.of(type)));
          }
        }
        if (columns.isEmpty()) {
          return Optional.absent();
        } else {
          return Optional.of(
              new TableImpl(databaseAndTable.getDatabase(), databaseAndTable.getTable(), columns)
          );
        }
      }
    } catch (SQLException e) {
      String err = String.format(
          "Error getting '%s.%s' schema",
          databaseAndTable.getDatabase(),
          databaseAndTable.getTable()
      );
      LOG.error(err, e);
      throw new SchemaLoadException(err, e);
    }
  }

  public static class SchemaLoadException extends RuntimeException {
    public SchemaLoadException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
