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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TableContext {

  private static final Logger LOG = LoggerFactory.getLogger(TableContext.class);

  private final String schema;
  private final String tableName;
  private final LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
  private final Map<String, String> offsetColumnToPartitionOffsetAdjustments = new HashMap<>();
  private final Map<String, String> offsetColumnToMinValues = new HashMap<>();
  private final boolean enableNonIncremental;
  private final PartitioningMode partitioningMode;
  private final int maxNumActivePartitions;
  private final String extraOffsetColumnConditions;
  private final boolean partitionable;
  private Map<String, String> offsetColumnToStartOffset = new HashMap<>();
  // optionally store all column labels and types
  private Map<String, Integer> columnToType = new LinkedHashMap<>();

  public TableContext(
      String schema,
      String tableName,
      LinkedHashMap<String, Integer> offsetColumnToType,
      Map<String, String> offsetColumnToStartOffset,
      Map<String, String> offsetColumnToPartitionOffsetAdjustments,
      Map<String, String> offsetColumnToMinValues,
      boolean enableNonIncremental,
      PartitioningMode partitioningMode,
      int maxNumActivePartitions,
      String extraOffsetColumnConditions
  ) {
    this.schema = schema;
    this.tableName = tableName;
    if (offsetColumnToType != null) {
      this.offsetColumnToType.putAll(offsetColumnToType);
    }
    if (offsetColumnToStartOffset != null) {
      this.offsetColumnToStartOffset.putAll(offsetColumnToStartOffset);
    }
    if (offsetColumnToMinValues != null) {
      this.offsetColumnToMinValues.putAll(offsetColumnToMinValues);
    }
    this.extraOffsetColumnConditions = extraOffsetColumnConditions;
    this.enableNonIncremental = enableNonIncremental;
    this.partitioningMode = partitioningMode;
    this.maxNumActivePartitions = maxNumActivePartitions;
    if (offsetColumnToPartitionOffsetAdjustments != null) {
      this.offsetColumnToPartitionOffsetAdjustments.putAll(offsetColumnToPartitionOffsetAdjustments);
    }
    this.partitionable = isPartitionable(this);
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

  public Map<String, Integer> getOffsetColumnToType() {
    return Collections.unmodifiableMap(offsetColumnToType);
  }

  public Set<String> getOffsetColumns() {
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

  public Map<String, String> getOffsetColumnToPartitionOffsetAdjustments() {
    return offsetColumnToPartitionOffsetAdjustments;
  }

  public Map<String, String> getOffsetColumnToMinValues() {
    return Collections.unmodifiableMap(offsetColumnToMinValues);
  }

  public boolean isNonIncrementalLoadRequired() {
    return isEnableNonIncremental() && getOffsetColumns().isEmpty();
  }

  public boolean isEnableNonIncremental() {
    return enableNonIncremental;
  }

  public boolean isPartitionable() {
    return partitionable;
  }

  public PartitioningMode getPartitioningMode() {
    return partitioningMode;
  }

  public int getMaxNumActivePartitions() {
    return maxNumActivePartitions;
  }

  //Used to reset after the first batch we should not be using the initial offsets.
  public void clearStartOffset() {
    offsetColumnToStartOffset.clear();
  }

  public String getExtraOffsetColumnConditions() {
    return extraOffsetColumnConditions;
  }

  private static boolean isPartitionable(TableContext sourceTableContext) {
    return isPartitionable(sourceTableContext, null);
  }

  public static boolean isPartitionable(TableContext sourceTableContext, List<String> outputReasons) {
    final String tableName = sourceTableContext.getQualifiedName();

    List<String> reasons = new LinkedList<>();

    if (sourceTableContext.isNonIncrementalLoadRequired()) {
      reasons.add(String.format(
          "Table %s is not partitionable because it requires using non-incremental loading",
          tableName
      ));
    }

    if (sourceTableContext.getOffsetColumns().size() > 1) {
      reasons.add(String.format(
          "Table %s is not partitionable because it has more than one offset column",
          tableName
      ));
    }

    for (Map.Entry<String, Integer> offsetColToType : sourceTableContext.getOffsetColumnToType().entrySet()) {
      final int type = offsetColToType.getValue();
      if (!TableContextUtil.PARTITIONABLE_TYPES.contains(type)) {
        reasons.add(String.format(
            "Table %s is not partitionable because %s column (type %s) is not partitionable",
            tableName,
            offsetColToType.getKey(),
            JDBCType.valueOf(type).getName()
        ));
      }

      if (!sourceTableContext.getOffsetColumnToMinValues().containsKey(offsetColToType.getKey())) {
        reasons.add(String.format(
            "Table %s is not partitionable because %s column (type %s) did not have a minimum value available at" +
                " pipeline start time; only tables with with at least one row can be partitioned",
            tableName,
            offsetColToType.getKey(),
            JDBCType.valueOf(type).getName()
        ));
      }
    }

    if (reasons.isEmpty()) {
      return true;
    } else {
      if (outputReasons != null) {
        outputReasons.addAll(reasons);
      }
      if (LOG.isDebugEnabled()) {
        for (String reason : reasons) {
          LOG.debug(reason);
        }
      }
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableContext that = (TableContext) o;
    return Objects.equals(schema, that.schema) && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, tableName);
  }

  @Override
  public String toString() {
    return String.format("TableContext{schema='%s', tableName='%s'}", schema, tableName);
  }

  public void setOffsetColumnToStartOffset(Map<String, String> offsetColumnToStartOffset) {
    this.offsetColumnToStartOffset = offsetColumnToStartOffset;
  }

  public void setColumnToType(Map<String, Integer> columnToType) {
    this.columnToType = columnToType;
  }

  public Map<String, Integer> getColumnToType() {
    return this.columnToType;
  }
}
