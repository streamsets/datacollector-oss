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
package com.streamsets.pipeline.stage.origin.jdbc.table.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableContextUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcELEvalContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class OffsetQueryUtil {
  private static final JsonMapper JSON_MAPPER = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
  private static final Logger LOG = LoggerFactory.getLogger(OffsetQueryUtil.class);

  private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ");

  private static final Joiner OR_JOINER = Joiner.on(" or ");
  private static final Joiner AND_JOINER = Joiner.on(" and ");

  private static final String TABLE_QUERY_SELECT = "select * from %s";

  private static final String COLUMN_GREATER_THAN_VALUE = "%s > %s";
  private static final String COLUMN_EQUALS_VALUE = "%s = %s";

  private static final String CONDITION_FORMAT = "( %s )";

  private static final String WHERE_CLAUSE = " WHERE %s ";
  private static final String ORDER_BY_CLAUSE = " ORDER by %s ";
  private static final String PREPARED_STATEMENT_POSITIONAL_PARAMETER = "?";


  private static final Joiner OFFSET_COLUMN_JOINER = Joiner.on("::");
  private static final Splitter OFFSET_COLUMN_SPLITTER = Splitter.on("::");
  private static final String OFFSET_COLUMN_NAME_VALUE = "%s=%s";
  public static final String QUOTED_NAME = "%s%s%s";

  public static final Set<Integer> UNSUPPORTED_OFFSET_SQL_TYPES = ImmutableSet.of(
      Types.BLOB,
      Types.BINARY,
      Types.VARBINARY,
      Types.LONGVARBINARY,
      Types.ARRAY,
      Types.DATALINK,
      Types.DISTINCT,
      Types.JAVA_OBJECT,
      Types.NULL,
      Types.OTHER,
      Types.REF,
      Types.SQLXML,
      Types.STRUCT
  );

  public static final Map<Integer, Field.Type> SQL_TYPE_TO_FIELD_TYPE =
      ImmutableMap.<Integer, Field.Type>builder()
          .put(Types.BIT, Field.Type.BYTE)
          .put(Types.BOOLEAN, Field.Type.BYTE)
          .put(Types.CHAR, Field.Type.STRING)
          .put(Types.VARCHAR, Field.Type.STRING)
          .put(Types.NCHAR, Field.Type.STRING)
          .put(Types.NVARCHAR, Field.Type.STRING)
          .put(Types.LONGNVARCHAR, Field.Type.STRING)
          .put(Types.LONGVARCHAR, Field.Type.STRING)
          .put(Types.TIME, Field.Type.TIME)
          .put(Types.DATE, Field.Type.DATE)
          .put(Types.TIMESTAMP, Field.Type.DATETIME)
          .put(Types.INTEGER, Field.Type.INTEGER)
          .put(Types.SMALLINT, Field.Type.SHORT)
          .put(Types.TINYINT, Field.Type.SHORT)
          .put(Types.BIGINT, Field.Type.LONG)
          .put(Types.FLOAT, Field.Type.FLOAT)
          .put(Types.REAL, Field.Type.FLOAT)
          .put(Types.DOUBLE, Field.Type.DOUBLE)
          .put(Types.DECIMAL, Field.Type.DECIMAL)
          .put(Types.NUMERIC, Field.Type.DECIMAL)
          .build();

  private OffsetQueryUtil() {}

  /**
   * Build query using the lastOffset which is of the form (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>)
   *
   * @param tableContext Context for the current table.
   * @param lastOffset the last offset for this particular table
   * @return A query to execute for the current batch.
   */
  public static Pair<String, List<Pair<Integer, String>>> buildAndReturnQueryAndParamValToSet(
      TableContext tableContext,
      String lastOffset,
      String quoteChar,
      TableJdbcELEvalContext tableJdbcELEvalContext
  ) throws ELEvalException {
    StringBuilder queryBuilder = new StringBuilder();
    List<Pair<Integer, String>> paramValueToSet = new ArrayList<>();
    queryBuilder.append(
        String.format(
            TABLE_QUERY_SELECT,
            TableContextUtil.getQuotedQualifiedTableName(
                tableContext.getSchema(),
                tableContext.getTableName(),
                quoteChar
            )
        )
    );

    Map<String, String> storedTableToOffset = getColumnsToOffsetMapFromOffsetFormat(lastOffset);

    //Determines whether an initial offset is specified in the config and there is no stored offset.
    boolean isOffsetOverriden = tableContext.isOffsetOverriden() && storedTableToOffset.isEmpty();

    Map<String, String> offset = isOffsetOverriden?
        //Use the offset in the configuration
        tableContext.getOffsetColumnToStartOffset() :
        // if offset is available
        // get the stored offset (which is of the form partitionName=value) and strip off 'offsetColumns=' prefix
        // else null
        storedTableToOffset;

    List<String> finalAndConditions = new ArrayList<>();
    //Apply last offset conditions
    if (offset != null && !offset.isEmpty()) {
      List<String> finalOrConditions = new ArrayList<>();
      List<String> preconditions = new ArrayList<>();
      List<Pair<Integer, String>> preconditionParamVals = new ArrayList<>();
      //For partition columns p1, p2 and p3 with offsets o1, o2 and o3 respectively, the query will look something like
      //select * from tableName where (p1 > o1) or (p1 = o1 and p2 > o2) or (p1 = o1 and p2 = o2 and p3 > o3) order by p1, p2, p3.
      for (String partitionColumn : tableContext.getOffsetColumns()) {
        int partitionSqlType = tableContext.getOffsetColumnType(partitionColumn);
        String partitionOffset = offset.get(partitionColumn);
        String conditionForThisPartitionColumn =
            getConditionForPartitionColumn(
                partitionColumn,
                true,
                preconditions,
                quoteChar
            );
        //Add for preconditions (EX: composite keys)
        paramValueToSet.addAll(new ArrayList<>(preconditionParamVals));
        Pair<Integer, String> paramValForCurrentOffsetColumn = Pair.of(partitionSqlType, partitionOffset);
        //Add for current partition column
        paramValueToSet.add(paramValForCurrentOffsetColumn);
        finalOrConditions.add(String.format(CONDITION_FORMAT, conditionForThisPartitionColumn));
        preconditions.add(
            getConditionForPartitionColumn(
                partitionColumn,
                false,
                Collections.emptyList(),
                quoteChar
            )
        );
        preconditionParamVals.add(paramValForCurrentOffsetColumn);
      }
      finalAndConditions.add(String.format(CONDITION_FORMAT, OR_JOINER.join(finalOrConditions)));
    }

    if (!StringUtils.isEmpty(tableContext.getExtraOffsetColumnConditions())) {
      //Apply extra offset column conditions configured which will be appended as AND on the query
      String condition =
          tableJdbcELEvalContext.evaluateAsString(
              "extraOffsetColumnConditions",
              tableContext.getExtraOffsetColumnConditions()
          );
      finalAndConditions.add(String.format(CONDITION_FORMAT, condition));
    }

    if (!finalAndConditions.isEmpty()) {
      queryBuilder.append(String.format(WHERE_CLAUSE, AND_JOINER.join(finalAndConditions)));
    }

    Collection<String> quotedOffsetColumns =
        tableContext.getOffsetColumns().stream().map(
            offsetCol -> String.format(QUOTED_NAME, quoteChar, offsetCol, quoteChar)
        ).collect(Collectors.toList());

    queryBuilder.append(String.format(ORDER_BY_CLAUSE, COMMA_SPACE_JOINER.join(quotedOffsetColumns)));
    return Pair.of(queryBuilder.toString(),paramValueToSet);
  }


  /**
   * Builds parts of the query in the where clause for the the partitition column.
   *
   * @param partitionColumn Partition Column
   * @param greaterThan Whether the conditiond needs to be greater than or equal.
   * @param preconditions Any other precondition in the specific condition that needs to be combined with partition column.
   *                      (For EX: if there are multiple order by we may need to say equals for columns
   *                      before this partition column and apply the current partition column conditions)
   * @return the constructed condition for the partition column
   */
  private static String getConditionForPartitionColumn(
      String partitionColumn,
      boolean greaterThan,
      List<String> preconditions,
      String quoteChar
  ) {
    String conditionTemplate = greaterThan? COLUMN_GREATER_THAN_VALUE : COLUMN_EQUALS_VALUE;
    List<String> finalConditions = new ArrayList<>(preconditions);
    finalConditions.add(
        String.format(
            conditionTemplate,
            String.format(QUOTED_NAME, quoteChar, partitionColumn, quoteChar),
            PREPARED_STATEMENT_POSITIONAL_PARAMETER
        )
    );
    return AND_JOINER.join(finalConditions);
  }

  /**
   * Splits the offset in the form of (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>) into a map of columns and values
   * @param lastOffset the last offset for the current table.
   * @return Map of columns to values
   */
  public static Map<String, String> getColumnsToOffsetMapFromOffsetFormat(String lastOffset) {
    Map<String, String> offsetColumnsToOffsetMap = new HashMap<>();
    if (lastOffset != null) {
      Iterator<String> offsetColumnsAndOffsetIterator = OFFSET_COLUMN_SPLITTER.split(lastOffset).iterator();
      while (offsetColumnsAndOffsetIterator.hasNext()) {
        String offsetColumnAndOffset = offsetColumnsAndOffsetIterator.next();
        String[] offsetColumnOffsetSplit = offsetColumnAndOffset.split("=");
        String offsetColumn = offsetColumnOffsetSplit[0];
        String offset = offsetColumnOffsetSplit[1];
        offsetColumnsToOffsetMap.put(offsetColumn, offset);
      }
    }
    return offsetColumnsToOffsetMap;
  }

  /**
   * Joins the map of column to values to a string offset in the form of (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>)
   * @param tableContext Context for the current table.
   * @param fields The current record fields
   * @return Offset in the form of (<column1>=<value1>::<column2>=<value2>::<column3>=<value3>)
   */
  public static String getOffsetFormatFromColumns(TableContext tableContext, Map<String, Field> fields) {
    List<String> offsetColumnFormat = new ArrayList<>();
    for (String offsetColumn : tableContext.getOffsetColumns()) {
      Field field = fields.get(offsetColumn);
      Object value = field.getType().isOneOf(Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME)?
          //For DATE/TIME fields store the long in string format and convert back to date when using offset
          //in query
          String.valueOf(field.getValueAsDatetime().getTime()) : field.getValue();
      offsetColumnFormat.add(String.format(OFFSET_COLUMN_NAME_VALUE, offsetColumn, value));
    }
    return OFFSET_COLUMN_JOINER.join(offsetColumnFormat);
  }

  /**
   * Serialize the Map of table to offset to a String
   * @param offsetMap Map of table to Offset.
   * @return Serialized offset
   * @throws StageException When Serialization exception happens
   */
  public static String serializeOffsetMap(Map<String, String> offsetMap) throws StageException {
    try {
      return JSON_MAPPER.writeValueAsString(offsetMap);
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
        offsetMap = JSON_MAPPER.readValue(lastSourceOffset, Map.class);
      } catch (IOException ex) {
        LOG.error("Error when deserializing", ex);
        throw new StageException(JdbcErrors.JDBC_61, ex);
      }
    }
    return offsetMap;
  }

  /**
   * Validates whether offset names match in the stored offset with respect to table configuration
   * @param tableContext {@link TableContext} for table
   * @param offset Stored offset from the previous stage run
   * @throws StageException if columns mismatch
   */
  public static void validateStoredAndSpecifiedOffset(TableContext tableContext, String offset) throws StageException {
    Set<String> expectedColumns = Sets.newHashSet(tableContext.getOffsetColumns());
    Set<String> actualColumns = getColumnsToOffsetMapFromOffsetFormat(offset).keySet();

    Set<String> expectedSetDifference = Sets.difference(expectedColumns, actualColumns);
    Set<String> actualSetDifference = Sets.difference(actualColumns, expectedColumns);

    if (expectedSetDifference.size() > 0 || actualSetDifference.size() >  0) {
      throw new StageException(
          JdbcErrors.JDBC_71,
          tableContext.getQualifiedName(),
          COMMA_SPACE_JOINER.join(actualColumns),
          COMMA_SPACE_JOINER.join(expectedColumns)
      );
    }
  }
}
