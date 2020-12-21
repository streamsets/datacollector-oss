/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc.parser.sql;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode;
import org.apache.commons.lang3.StringUtils;
import org.parboiled.Node;
import org.parboiled.Rule;
import org.parboiled.parserunners.BasicParseRunner;
import org.parboiled.parserunners.ParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.lib.jdbc.parser.sql.SQLParser.COLUMN_NAME_RULE;
import static com.streamsets.pipeline.lib.jdbc.parser.sql.SQLParser.COLUMN_VALUE_RULE;

public class SQLParserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SQLParserUtils.class);
  private static final String NULL_STRING = "NULL";

  private SQLParserUtils() {}

  @SuppressWarnings("unchecked")
  public static Map<String, String> process(
      SQLParser parser,
      String sql,
      int type, // One of OracleCDCOperationCode constants.
      boolean allowNulls,
      boolean caseSensitive,
      Set<String> columnsExpected
  ) throws UnparseableSQLException {
    if (StringUtils.isEmpty(sql)) {
      throw new UnparseableEmptySQLException(sql);
    }
    Rule parseRule;
    switch (type) {
      case OracleCDCOperationCode.INSERT_CODE:
         parseRule = parser.Insert();
         break;
      case OracleCDCOperationCode.UPDATE_CODE:
      case OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE:
        parseRule = parser.Update();
        break;
      case OracleCDCOperationCode.DELETE_CODE:
        parseRule = parser.Delete();
        break;
      default:
        throw new UnparseableSQLException(sql);
    }
    ParseRunner<?> runner = new BasicParseRunner<>(parseRule);
    ParsingResult<?> result = runner.run(sql);
    if (!result.matched) {
      throw new UnparseableSQLException(sql);
    }
    Collection<Node<Object>> names = new ArrayList<>();

    ParseTreeUtils.collectNodes(
        (Node<Object>) result.parseTreeRoot,
        input -> input.getLabel().equals(COLUMN_NAME_RULE),
        names
    );

    Collection<Node<Object>> values = new ArrayList<>();
    ParseTreeUtils.collectNodes(
        (Node<Object>) result.parseTreeRoot,
        input -> input.getLabel().equals(COLUMN_VALUE_RULE),
        values
    );

    Map<String, String> colVals = new HashMap<>();
    for (int i = 0; i < names.size(); i++) {
      Node<?> name = ((ArrayList<Node<Object>>) names).get(i);
      Node<?> val = ((ArrayList<Node<Object>>) values).get(i);

      final String colName = sql.substring(name.getStartIndex(), name.getEndIndex());
      final String key = formatName(colName, caseSensitive);
      if (!colVals.containsKey(key)) {
        colVals.put(key, formatValue(sql.substring(val.getStartIndex(), val.getEndIndex())));
      }
    }
    if (allowNulls && columnsExpected != null) {
      columnsExpected.forEach(col -> colVals.putIfAbsent(col,  null));
    }
    return colVals;
  }

  /**
   * Format column names based on whether they are case-sensitive
   */
  private static String formatName(String columnName, boolean caseSensitive) {
    String returnValue = format(columnName);
    if (caseSensitive) {
      return returnValue;
    }
    return returnValue.toUpperCase();
  }

  /**
   * Unescapes strings and returns them.
   */
  private static String formatValue(String value) {
    // The value can either be null (if the IS keyword is present before it or just a NULL string with no quotes)
    if (value == null || NULL_STRING.equalsIgnoreCase(value)) {
      return null;
    }
    String returnValue = format(value);
    return returnValue.replaceAll("''", "'");
  }

  @VisibleForTesting
  public static String format(String columnName) {
    int stripCount;

    if (columnName.startsWith("\"\'")) {
      stripCount = 2;
    } else if (columnName.startsWith("\"") || columnName.startsWith("\'")) {
      stripCount = 1;
    } else {
      return columnName;
    }
    return columnName.substring(stripCount, columnName.length() - stripCount);
  }

}
