/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestTableContextUtilUpdateOffsetMapMinMax {
  public static final String DB_NAME = "db";
  public static final String TABLE_NAME = "table1";
  public static final String COLUMN_NAME = "col1";

  // Will be used in the min/max comparisons
  public static final String INTEGER_FIVE = String.valueOf(5);
  public static final String INTEGER_TEN = String.valueOf(10);
  public static final String SMALLER_TIMESTAMP = "1559184471";
  public static final String GREATER_TIMESTAMP = "1559184473";

  private final TableContext sourceTable;
  private final Map<String, String> offsetColumnToStartOffset;
  private final Map<String, String> offsetColumnToMinValues;

  private final OffsetComparisonType comparisonType;
  private final Map<String, String> expectedOffsetMap;

  public TestTableContextUtilUpdateOffsetMapMinMax(
      TableContext sourceTable,
      Map<String, String> offsetColumnToStartOffset,
      Map<String, String> offsetColumnToMinValues,
      OffsetComparisonType comparisonType,
      Map<String, String> expectedOffsetMap
  ) {
    this.sourceTable = sourceTable;
    this.offsetColumnToStartOffset = offsetColumnToStartOffset;
    this.offsetColumnToMinValues = offsetColumnToMinValues;
    this.comparisonType = comparisonType;
    this.expectedOffsetMap = expectedOffsetMap;
  }

  @Parameterized.Parameters(
      name = "source Table Context: {0}, Starting Offset map: {1}, Map of Offset constants: {2}, " +
          " JDBC datatype: {3}," +
          " Comparision operator: {4}" +
          " Expected Offset map: {5}"
  )
  public static Collection<Object[]> data() {
    final List<Object[]> data = new LinkedList<>();
    Map<String, String> inputMap;
    TableContext table;

    // When data type is INTEGER
    inputMap = new HashMap<String, String>() {{
      put(COLUMN_NAME, INTEGER_FIVE);
    }};
    Map<String, String> compareMapInteger = Collections.singletonMap(COLUMN_NAME, INTEGER_FIVE);
    table = createTableContext(inputMap, compareMapInteger, Types.INTEGER);
    data.add(new Object[] {table, inputMap, compareMapInteger,
                          OffsetComparisonType.MINIMUM, Collections.singletonMap(COLUMN_NAME, INTEGER_FIVE)});

    // inputMap is potentially updated by call to updateOffsetMapwithMinMax. Reset it again for MAX comparison now
    inputMap = new HashMap<String, String>() {{
      put(COLUMN_NAME, INTEGER_TEN);
    }};
    data.add(new Object[] {table, inputMap, compareMapInteger,
        OffsetComparisonType.MAXIMUM, Collections.singletonMap(COLUMN_NAME, INTEGER_TEN)});


    // When data type is TIMESTAMP
    inputMap = new HashMap<String, String>() {{
      put(COLUMN_NAME, GREATER_TIMESTAMP);
    }};
    Map<String, String> compareMapTimestamp = Collections.singletonMap(COLUMN_NAME, SMALLER_TIMESTAMP);
    table = createTableContext(inputMap, compareMapTimestamp, Types.TIMESTAMP);
    data.add(new Object[] {table, inputMap, compareMapTimestamp,
                          OffsetComparisonType.MINIMUM, Collections.singletonMap(COLUMN_NAME, SMALLER_TIMESTAMP)});

    // inputMap is potentially updated by call to updateOffsetMapwithMinMax. Reset it again for MAX comparison now
    inputMap = new HashMap<String, String>() {{
      put(COLUMN_NAME, GREATER_TIMESTAMP);
    }};
    data.add(new Object[] {table, inputMap, compareMapTimestamp,
                          OffsetComparisonType.MAXIMUM, Collections.singletonMap(COLUMN_NAME, GREATER_TIMESTAMP)});

    return data;
  }

  /* Helper function for generating table context */
  @NotNull
  protected static TableContext createTableContext(
      Map<String, String> offsetColumnToStartOffset,
      Map<String, String> offsetColumnToMinValues,
      int jdbcType
  ) {
    LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();

    offsetColumnToType.put(COLUMN_NAME, jdbcType);

    return new TableContext(
      DatabaseVendor.UNKNOWN,
      QuoteChar.NONE,
      DB_NAME,
      TABLE_NAME,
      offsetColumnToType,
      offsetColumnToStartOffset,
      Collections.emptyMap(),
      offsetColumnToMinValues,
      Collections.emptyMap(),
      false,
      PartitioningMode.REQUIRED,
      10,
      null,
      0L
    );
  }

  @Test
  public void updateOffsetMapwithMinMax() {
    TableContextUtil.updateOffsetMapwithMinMax(
        sourceTable,
        offsetColumnToStartOffset,
        offsetColumnToMinValues,
        comparisonType
    );
    assertEquals(offsetColumnToStartOffset, expectedOffsetMap);
  }
}