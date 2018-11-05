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
package com.streamsets.pipeline.stage.origin.jdbc.cdc;

import org.apache.commons.lang3.StringUtils;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * This class is to keep schema and table relation.
 */
public class SchemaAndTable {

  private final String schema;
  private final String table;

  public SchemaAndTable(String schema, String table) {
    this.schema = schema;
    this.table = table;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public boolean isNotEmpty() {
    return StringUtils.isNotEmpty(this.schema) && StringUtils.isNotEmpty(this.table);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SchemaAndTable) {
      SchemaAndTable sat = (SchemaAndTable) o;
      return StringUtils.equals(sat.getSchema(), this.schema) &&
              StringUtils.equals(sat.getTable(), this.table);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.schema.hashCode() + this.table.hashCode();
  }

  public String toString() {
    return Utils.format("Schema = '{}', Table = '{}'", schema, table);
  }
}
