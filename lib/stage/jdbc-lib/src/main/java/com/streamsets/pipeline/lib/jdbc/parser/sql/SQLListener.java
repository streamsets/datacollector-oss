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
package com.streamsets.pipeline.lib.jdbc.parser.sql;

import com.google.common.annotations.VisibleForTesting;
import org.antlr.v4.runtime.tree.ParseTree;
import plsql.plsqlBaseListener;
import plsql.plsqlParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Listener for use with {@linkplain org.antlr.v4.runtime.tree.ParseTreeWalker}.
 */
public class SQLListener extends plsqlBaseListener {

  private final HashMap<String, String> columns = new HashMap<>();
  private static final String NULL_STRING = "NULL";
  private boolean insideStatement = false;
  private boolean caseSensitive = false;
  private boolean allowNulls = false;
  private Set<String> columnsExpected;
  private String table;
  private String schema;

  private final Pattern tableSchemaPattern = Pattern.compile("\"(.*)\"\\.\"(.*)\"");

  private List<plsqlParser.Column_nameContext> columnNames;

  @Override
  public void enterUpdate_set_clause(plsqlParser.Update_set_clauseContext ctx) {
    for(plsqlParser.Column_based_update_set_clauseContext x : ctx.column_based_update_set_clause()) {
      columns.put(formatName(x.column_name(0).getText().trim()), formatValue(x.expression().getText().trim()));
    }
  }

  private void extractTableAndSchema(String tableSchema) {
    Matcher m = tableSchemaPattern.matcher(tableSchema);
    if (m.matches()) {
      schema = m.group(1);
      table = m.group(2);
    } else {
      table = format(table); // no schema name, only table
    }
  }

  @Override
  public void enterUpdate_statement(plsqlParser.Update_statementContext ctx) {
    if (table == null) {
      String tableSchema = ctx.general_table_ref().getText();
      extractTableAndSchema(tableSchema);
    }
  }

  @Override
  public void enterDelete_statement(plsqlParser.Delete_statementContext ctx) {
    if (table == null) {
      String tableSchema = ctx.general_table_ref().getText();
      extractTableAndSchema(tableSchema);
    }
  }

  @Override
  public void enterInsert_into_clause(plsqlParser.Insert_into_clauseContext ctx) {
    if (table == null) {
      String tableSchema = ctx.general_table_ref().getText();
      extractTableAndSchema(tableSchema);
    }

    this.columnNames = ctx.column_name();
  }

  @Override
  public void enterValues_clause(plsqlParser.Values_clauseContext ctx) {
    List<plsqlParser.ExpressionContext> expressions = ctx.expression_list().expression();
    for (int i = 0; i < expressions.size(); i++) {
      columns.put(formatName(columnNames.get(i).getText().trim()), formatValue(expressions.get(i).getText().trim()));
    }
  }

  @Override
  public void enterWhere_clause(plsqlParser.Where_clauseContext ctx) {
    insideStatement = true;
  }

  @Override
  public void exitWhere_clause(plsqlParser.Where_clauseContext ctx) {
    insideStatement = false;
  }

  @Override
  public void enterEquality_expression(plsqlParser.Equality_expressionContext ctx) {
    if (insideStatement) {
      // This is pretty horrible, but after some experimentation, I figured that the
      // third level of the tree contained the actual data. I am assuming it is because
      // top level is actually empty root, 2nd level contains the actual node, and 3rd level
      // has its individual tokens -> 0 is key, 1 is = and 2 is the value.
      String key = null;
      String val = null;
      ParseTree level0 = ctx.getChild(0);
      if (level0 != null) {
        ParseTree level1 = level0.getChild(0);
        if (level1 != null) {
          ParseTree keyNode = level1.getChild(0);
          if (keyNode != null) {
            key = formatName(keyNode.getText());
          }
          ParseTree valNode = level1.getChild(2);
          if (valNode != null) {
            val = valNode.getText();
          }
        }
      }
      // Why check the table's column names? Because stuff like TO_DATE(<something>) will also come in here
      // with each token as a key with null value.
      if (key != null
          && (val != null || (allowNulls && columnsExpected != null && columnsExpected.contains(key)))
          && !columns.containsKey(key)) {
        columns.put(key, formatValue(val));
      }
    }
  }

  /**
   * Format column names based on whether they are case-sensitive
   */
  private String formatName(String columnName) {
    String returnValue = format(columnName);
    if (caseSensitive) {
      return returnValue;
    }
    return returnValue.toUpperCase();
  }

  /**
   * Unescapes strings and returns them.
   */
  private String formatValue(String value) {
    // The value can either be null (if the IS keyword is present before it or just a NULL string with no quotes)
    if (value == null || NULL_STRING.equalsIgnoreCase(value)) {
      return null;
    }
    String returnValue = format(value);
    return returnValue.replaceAll("''", "'");
  }

  @VisibleForTesting
  public String format(String columnName) {
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

  /**
   * Reset the listener to use with the next statement. All column information is cleared.
   */
  public void reset(){
    columns.clear();
    this.columnsExpected = null;
    columnNames = null;
    table = null;
    schema = null;
    insideStatement = false;
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }


  public void setCaseSensitive() {
    this.caseSensitive = true;
  }

  public void allowNulls() {
    this.allowNulls = true;
  }

  public void setColumns(Set<String> columns) {
    this.columnsExpected = columns;
  }
}
