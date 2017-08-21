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
 * MySql table column description.
 *
 * Note: column name is case-sensitive.
 */
public class Column {
  private final String name;
  private final MysqlType type;

  public Column(String name, MysqlType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public MysqlType getType() {
    return type;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Column{");
    sb.append("name='").append(name).append('\'');
    sb.append(", type=").append(type);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Column)) {
      return false;
    }

    Column column = (Column) o;

    if (name != null ? !name.equals(column.name) : column.name != null) {
      return false;
    }
    return type == column.type;

  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
  }
}
