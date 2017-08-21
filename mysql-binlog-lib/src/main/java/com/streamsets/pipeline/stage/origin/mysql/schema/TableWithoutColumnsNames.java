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

/**
 * Table metadata with columns names and types. When MySql does not have metadata for some table
 * (the table has been dropped) this may be used - columns will have names <code>col_N</code>
 * and data type {@link MysqlType#TEXT}.
 */
public class TableWithoutColumnsNames implements Table {
  private final String database;
  private final String name;

  public TableWithoutColumnsNames(String database, String name) {
    this.database = database;
    this.name = name;
  }

  @Override
  public Column getColumn(int position) {
    return new Column(String.format("col_%d", position), MysqlType.TEXT);
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TableWithoutColumnsNames{");
    sb.append("database='").append(database).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableWithoutColumnsNames)) {
      return false;
    }

    TableWithoutColumnsNames that = (TableWithoutColumnsNames) o;

    if (database != null ? !database.equals(that.database) : that.database != null) {
      return false;
    }
    return name != null ? name.equals(that.name) : that.name == null;

  }

  @Override
  public int hashCode() {
    int result = database != null ? database.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
