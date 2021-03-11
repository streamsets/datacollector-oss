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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plsql.plsqlLexer;
import plsql.plsqlParser;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public final class ParseUtil {

  // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPTZ
  private static final int TIMESTAMP_TZ_TYPE = -101;
  // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPLTZ
  private static final int TIMESTAMP_LTZ_TYPE = -102;

  public static final String EMPTY_STRING = "";
  public static final String NULL_VALUE = null;

  public static final Map<Integer, String> JDBCTypeNames = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(ParseUtil.class);

  static {
    for (java.lang.reflect.Field jdbcType : Types.class.getFields()) {
      try {
        JDBCTypeNames.put((Integer) jdbcType.get(null), jdbcType.getName());
      } catch (Exception ex) {
        LOG.warn("JDBC Type Name access error", ex);
      }
    }
  }


  private ParseUtil() {
  }

  public static ParserRuleContext getParserRuleContext(
      String queryString,
      int op
  ) throws UnparseableSQLException {
    plsqlLexer lexer = new plsqlLexer(new ANTLRInputStream(queryString));
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    plsqlParser parser = new plsqlParser(tokenStream);
    ParserRuleContext context = null;
    switch (op) {
      case OracleCDCOperationCode.UPDATE_CODE:
      case OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE:
        context = parser.update_statement();
        break;
      case OracleCDCOperationCode.INSERT_CODE:
        context = parser.insert_statement();
        break;
      case OracleCDCOperationCode.DELETE_CODE:
        context = parser.delete_statement();
        break;
      case OracleCDCOperationCode.DDL_CODE:
      case OracleCDCOperationCode.COMMIT_CODE:
      case OracleCDCOperationCode.ROLLBACK_CODE:
        break;
      default:
        throw new UnparseableSQLException(queryString);
    }
    return context;
  }

  public static int getOperation(int op) {
    switch (op) {
      case OracleCDCOperationCode.UPDATE_CODE:
      case OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE:
        return OperationType.UPDATE_CODE;
      case OracleCDCOperationCode.INSERT_CODE:
        return OperationType.INSERT_CODE;
      case OracleCDCOperationCode.DELETE_CODE:
        return OperationType.DELETE_CODE;
      case OracleCDCOperationCode.DDL_CODE:
      case OracleCDCOperationCode.COMMIT_CODE:
      case OracleCDCOperationCode.ROLLBACK_CODE:
      default:
        return -1;
    }
  }

  public static String giveActualValue(boolean isEmptyStringEqualsNull, String value) {
    if (isEmptyStringEqualsNull && value != null && value.equals(EMPTY_STRING)) {
      return NULL_VALUE;
    }
    return value;
  }

  public static Field generateField(
      boolean isEmptyStringEqualsNull,
      String column,
      String columnValue,
      int columnType,
      DateTimeColumnHandler dateTimeColumnHandler
  ) throws StageException {
    return generateField(
        column,
        giveActualValue(isEmptyStringEqualsNull, columnValue),
        columnType,
        dateTimeColumnHandler);
  }

  public static Field generateField(
      String column,
      String columnValue,
      int columnType,
      DateTimeColumnHandler dateTimeColumnHandler
  ) throws StageException {
    Field field;
    // All types as of JDBC 2.0 are here:
    // https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
    // Good source of recommended mappings is here:
    // http://www.cs.mun.ca/java-api-1.5/guide/jdbc/getstart/mapping.html
    switch (columnType) {
      case Types.BIGINT:
        field = Field.create(Field.Type.LONG, columnValue);
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        field = Field.create(Field.Type.BYTE_ARRAY, RawTypeHandler.parseRaw(column, columnValue, columnType));
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        field = Field.create(Field.Type.BOOLEAN, columnValue);
        break;
      case Types.CHAR:
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        field = Field.create(Field.Type.STRING, columnValue);
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        field = Field.create(Field.Type.DECIMAL, columnValue);
        break;
      case Types.DOUBLE:
        field = Field.create(Field.Type.DOUBLE, columnValue);
        break;
      case Types.FLOAT:
      case Types.REAL:
        field = Field.create(Field.Type.FLOAT, columnValue);
        break;
      case Types.INTEGER:
        field = Field.create(Field.Type.INTEGER, columnValue);
        break;
      case Types.SMALLINT:
      case Types.TINYINT:
        field = Field.create(Field.Type.SHORT, columnValue);
        break;
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        field = dateTimeColumnHandler.getDateTimeStampField(column, columnValue, columnType);
        break;
      case Types.TIMESTAMP_WITH_TIMEZONE:
      case TIMESTAMP_TZ_TYPE:
        field = dateTimeColumnHandler.getTimestampWithTimezoneField(columnValue);
        break;
      case TIMESTAMP_LTZ_TYPE:
        field = dateTimeColumnHandler.getTimestampWithLocalTimezone(columnValue);
        break;
      case Types.ROWID:
      case Types.CLOB:
      case Types.NCLOB:
      case Types.BLOB:
      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.NULL:
      case Types.OTHER:
      case Types.REF:
      case Types.REF_CURSOR:
      case Types.SQLXML:
      case Types.STRUCT:
      case Types.TIME_WITH_TIMEZONE:
      default:
        throw new UnsupportedFieldTypeException(column, columnValue, columnType);
    }
    return field;
  }

}