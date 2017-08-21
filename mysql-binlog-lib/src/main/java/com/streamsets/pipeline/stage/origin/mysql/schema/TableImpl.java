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

import java.util.List;

public class TableImpl implements Table {
  private final String database;
  private final String name;
  private final List<Column> columns;

  public TableImpl(String database, String name, List<Column> columns) {
    this.database = database;
    this.name = name;
    this.columns = columns;
  }

  @Override
  public Column getColumn(int position) {
    return columns.get(position);
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
    final StringBuilder sb = new StringBuilder("Table{");
    sb.append("columns=").append(columns);
    sb.append(", database='").append(database).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableImpl)) {
      return false;
    }

    TableImpl table = (TableImpl) o;

    if (database != null ? !database.equals(table.database) : table.database != null) {
      return false;
    }
    if (name != null ? !name.equals(table.name) : table.name != null) {
      return false;
    }
    return columns != null ? columns.equals(table.columns) : table.columns == null;

  }

  @Override
  public int hashCode() {
    int result = database != null ? database.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    return result;
  }
}
