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
package com.streamsets.pipeline.stage.origin.jdbc.table.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableContextUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class OffsetQueryUtil {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(OffsetQueryUtil.class);

  private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ");
  private static final Joiner OR_JOINER = Joiner.on(" or ");
  private static final Joiner AND_JOINER = Joiner.on(" and ");

  private static final String TABLE_QUERY_SELECT = "select * from %s";

  private static final String COLUMN_GREATER_THAN_WITHOUT_QUOTES = "%s > %s";
  private static final String COLUMN_GREATER_THAN_WITH_QUOTES = "%s > '%s'";

  private static final String COLUMN_EQUALS_VALUE_WITHOUT_QUOTES = "%s = %s";
  private static final String COLUMN_EQUALS_VALUE_WITH_QUOTES = "%s = '%s'";

  private static final String CONDITION_FORMAT = "( %s )";

  private static final String WHERE_CLAUSE = " WHERE %s ";
  private static final String ORDER_BY_CLAUSE = " ORDER by %s ";

  private static final Joiner PARTITION_OFFSET_JOINER = Joiner.on("::");
  private static final Splitter PARTITION_OFFSET_SPLITTER = Splitter.on("::");
  private static final String PARTITION_NAME_VALUE = "%s=%s";

  private OffsetQueryUtil() {}

  /**
   * Build query using the lastOffset which is of the form (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>)
   *
   * @param tableContext Context for the current table.
   * @param lastOffset the last offset for this particular table
   * @return A query to execute for the current batch.
   */
  public static String buildQuery(TableContext tableContext, String lastOffset) {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(
        String.format(
            TABLE_QUERY_SELECT,
            TableContextUtil.getQualifiedTableName(tableContext.getSchema(), tableContext.getTableName()))
    );

    Map<String, String> offset = (tableContext.isPartitionOffsetOverride())?
        //Use the offset in the configuration
        tableContext.getPartitionColumnToStartOffset() :
        // if offset is available
        // get the stored offset (which is of the form partitionName=value) and strip off 'partitionColumns=' prefix
        // else null
        getPartitionColumnsToOffsetMapFromOffsetFormat(lastOffset);

    if (offset != null && !offset.isEmpty()) {
      List<String> finalConditions = new ArrayList<>();
      List<String> preconditions = new ArrayList<>();
      //For partition columns p1, p2 and p3 with offsets o1, o2 and o3 respectively, the query will look something like
      //select * from tableName where (p1 > o1) or (p1 = o1 and p2 > o2) or (p1 = o1 and p2 = o2 and p3 > o3) order by p1, p2, p3.
      for (String partitionColumn : tableContext.getPartitionColumns()) {
        int partitionSqlType = tableContext.getPartitionType(partitionColumn);
        String partitionOffset = offset.get(partitionColumn);
        String conditionForThisPartitionColumn = getConditionForPartitionColumn(partitionColumn, partitionOffset, partitionSqlType, true, preconditions);
        finalConditions.add(String.format(CONDITION_FORMAT, conditionForThisPartitionColumn));
        preconditions.add(getConditionForPartitionColumn(partitionColumn, partitionOffset, partitionSqlType, false, Collections.<String>emptyList()));
      }
      queryBuilder.append(String.format(WHERE_CLAUSE, OR_JOINER.join(finalConditions)));
    }
    queryBuilder.append(String.format(ORDER_BY_CLAUSE, COMMA_SPACE_JOINER.join(tableContext.getPartitionColumns())));
    return queryBuilder.toString();
  }


  /**
   * Builds parts of the query in the where clause for the the partitition column.
   *
   * @param partitionColumn Partition Column
   * @param offset the value needed in the condition for the partition column
   * @param partitionType SQL Type of the partition column
   * @param greaterThan Whether the conditiond needs to be greater than or equal.
   * @param preconditions Any other precondition in the specific condition that needs to be combined with partition column.
   *                      (For EX: if there are multiple order by we may need to say equals for columns
   *                      before this partition column and apply the current partition column conditions)
   * @return the constructed condition for the partition column
   */
  private static String getConditionForPartitionColumn(String partitionColumn, String offset, int partitionType, boolean greaterThan, List<String> preconditions) {
    String conditionTemplate;
    //For Char, Varchar, date, time and timestamp embed the value in a quote
    if (TableContextUtil.isSqlTypeOneOf(partitionType, Types.CHAR, Types.VARCHAR, Types.DATE, Types.TIME, Types.TIMESTAMP)) {
      conditionTemplate = greaterThan? COLUMN_GREATER_THAN_WITH_QUOTES : COLUMN_EQUALS_VALUE_WITH_QUOTES;
    }  else {
      conditionTemplate = greaterThan? COLUMN_GREATER_THAN_WITHOUT_QUOTES : COLUMN_EQUALS_VALUE_WITHOUT_QUOTES;
    }
    List<String> finalConditions = new ArrayList<>(preconditions);
    finalConditions.add(String.format(conditionTemplate, partitionColumn, offset));
    return AND_JOINER.join(finalConditions);
  }

  /**
   * Splits the offset in the form of (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>) into a map of columns and values
   * @param lastOffset the last offset for the current table.
   * @return Map of columns to values
   */
  private static Map<String, String> getPartitionColumnsToOffsetMapFromOffsetFormat(String lastOffset) {
    Map<String, String> partitionColumnsToOffsetMap = new HashMap<>();
    if (lastOffset != null) {
      Iterator<String> partitionColumnsOffsetIterator = PARTITION_OFFSET_SPLITTER.split(lastOffset).iterator();
      while (partitionColumnsOffsetIterator.hasNext()) {
        String partitionColumnOffset = partitionColumnsOffsetIterator.next();
        String[] partitionColumnOffsetSplit = partitionColumnOffset.split("=");
        String partitionColumn = partitionColumnOffsetSplit[0];
        String partitionOffset = partitionColumnOffsetSplit[1];
        partitionColumnsToOffsetMap.put(partitionColumn, partitionOffset);
      }
    }
    return partitionColumnsToOffsetMap;
  }

  /**
   * Joins the map of column to values to a string offset in the form of (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>)
   * @param tableContext Context for the current table.
   * @param fields The current record fields
   * @return Offset in the form of (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>)
   */
  public static String getOffsetFormatForPartitionColumns(TableContext tableContext, Map<String, Field> fields) {
    List<String> partitionColumnOffsetFormat = new ArrayList<>();
    for (String partitionColumn : tableContext.getPartitionColumns()) {
      partitionColumnOffsetFormat.add(String.format(PARTITION_NAME_VALUE, partitionColumn, fields.get(partitionColumn).getValue()));
    }
    return PARTITION_OFFSET_JOINER.join(partitionColumnOffsetFormat);
  }

  /**
   * Serialize the Map of table to offset to a String
   * @param offsetMap Map of table to Offset.
   * @return Serialized offset
   * @throws StageException When Serialization exception happens
   */
  public static String serializeOffsetMap(Map<String, String> offsetMap) throws StageException {
    try {
      return OBJECT_MAPPER.writeValueAsString(offsetMap);
    } catch (IOException ex) {
      LOG.error("Error when serializing", ex);
      throw new StageException(JdbcErrors.JDBC_60, ex);
    }
  }

  /**
   * Deserialize String offset to Map of table to offset
   * @param lastSourceOffset Serialized offset String
   * @return Map of table to lastOffset
   * @throws StageException When Deserialization exception happens
   */
  @SuppressWarnings("unchecked")
  public static Map<String, String> deserializeOffsetMap(String lastSourceOffset) throws StageException {
    Map<String, String> offsetMap;
    if (StringUtils.isEmpty(lastSourceOffset)) {
      offsetMap = new HashMap<>();
    } else {
      try {
        offsetMap = OBJECT_MAPPER.readValue(lastSourceOffset, Map.class);
      } catch (IOException ex) {
        LOG.error("Error when deserializing", ex);
        throw new StageException(JdbcErrors.JDBC_61, ex);
      }
    }
    return offsetMap;
  }

}
