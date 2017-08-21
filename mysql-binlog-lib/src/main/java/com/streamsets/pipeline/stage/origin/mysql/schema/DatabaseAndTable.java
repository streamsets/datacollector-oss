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
package com.streamsets.pipeline.stage.origin.mysql.schema;

public class DatabaseAndTable {
  private final String database;
  private final String table;

  public DatabaseAndTable(String database, String table) {
    this.database = database;
    this.table = table;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DatabaseAndTable{");
    sb.append("database='").append(database).append('\'');
    sb.append(", table='").append(table).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatabaseAndTable)) {
      return false;
    }

    DatabaseAndTable that = (DatabaseAndTable) o;

    if (database != null ? !database.equals(that.database) : that.database != null) {
      return false;
    }
    return table != null ? table.equals(that.table) : that.table == null;

  }

  @Override
  public int hashCode() {
    int result = database != null ? database.hashCode() : 0;
    result = 31 * result + (table != null ? table.hashCode() : 0);
    return result;
  }
}
