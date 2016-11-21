/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Map;

public final class TableContext {

  private final String schema;
  private final String tableName;
  private final LinkedHashMap<String, Integer> partitionColumnToType;
  private final Map<String, String> partitionColumnToStartOffset;
  private final String extraOffsetColumnConditions;

  TableContext(
      String schema,
      String tableName,
      LinkedHashMap<String, Integer> partitionColumnToType,
      Map<String, String> partitionColumnToStartOffset,
      String extraOffsetColumnConditions
  ) {
    this.schema = schema;
    this.tableName = tableName;
    this.partitionColumnToType = partitionColumnToType;
    this.partitionColumnToStartOffset = partitionColumnToStartOffset;
    this.extraOffsetColumnConditions = extraOffsetColumnConditions;
  }

  public String getSchema() {
    return this.schema;
  }

  public String getTableName() {
    return tableName;
  }

  public Collection<String> getPartitionColumns() {
    return partitionColumnToType.keySet();
  }

  public int getPartitionType(String partitionColumn) {
    return partitionColumnToType.get(partitionColumn);
  }

  public boolean isPartitionOffsetOverride() {
    return !partitionColumnToStartOffset.isEmpty();
  }

  public Map<String, String> getPartitionColumnToStartOffset() {
    return partitionColumnToStartOffset;
  }

  //Used to reset after the first batch we should not be using the initial offsets.
  public void clearStartOffset() {
    partitionColumnToStartOffset.clear();
  }

  public String getExtraOffsetColumnConditions() {
    return extraOffsetColumnConditions;
  }
}
