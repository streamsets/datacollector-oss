/**
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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TableContext {

  private final String schema;
  private final String tableName;
  private final LinkedHashMap<String, Integer> offsetColumnToType;
  private final Map<String, String> offsetColumnToStartOffset;
  private final String extraOffsetColumnConditions;

  TableContext(
      String schema,
      String tableName,
      LinkedHashMap<String, Integer> offsetColumnToType,
      Map<String, String> offsetColumnToStartOffset,
      String extraOffsetColumnConditions
  ) {
    this.schema = schema;
    this.tableName = tableName;
    this.offsetColumnToType = offsetColumnToType;
    this.offsetColumnToStartOffset = offsetColumnToStartOffset;
    this.extraOffsetColumnConditions = extraOffsetColumnConditions;
  }

  public String getSchema() {
    return this.schema;
  }

  public String getTableName() {
    return tableName;
  }

  public String getQualifiedName() {
    return TableContextUtil.getQualifiedTableName(schema, tableName);
  }

  public Collection<String> getOffsetColumns() {
    return offsetColumnToType.keySet();
  }

  public int getOffsetColumnType(String partitionColumn) {
    return offsetColumnToType.get(partitionColumn);
  }

  public boolean isOffsetOverriden() {
    return !offsetColumnToStartOffset.isEmpty();
  }

  public Map<String, String> getOffsetColumnToStartOffset() {
    return offsetColumnToStartOffset;
  }

  //Used to reset after the first batch we should not be using the initial offsets.
  public void clearStartOffset() {
    offsetColumnToStartOffset.clear();
  }

  public String getExtraOffsetColumnConditions() {
    return extraOffsetColumnConditions;
  }
}
