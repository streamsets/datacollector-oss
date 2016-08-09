/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.zaxxer.hikari.HikariDataSource;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plsql.plsqlLexer;
import plsql.plsqlParser;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_16;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_40;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_41;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_43;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_44;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_45;

public class OracleCDCSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCDCSource.class);
  private static final String CDB_ROOT = "CDB$ROOT";
  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String USERNAME = HIKARI_CONFIG_PREFIX + "username";
  private static final String COMMA = ",";
  private static final String QUOTE = "'";
  private static final String CURRENT_SCN =
      "SELECT MAX(ktuxescnw * POWER(2, 32) + ktuxescnb) FROM sys.x$ktuxe";
  private String redoLogEntriesSql = null;
  private static final String START_SCN_OPT_STR = "STARTSCN => ";
  private static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";
  private static final String PREFIX = "oracle.cdc.";
  private static final String SCN = PREFIX + "SCN";
  private static final String USER = PREFIX + "USER";
  private static final String OPERATION = PREFIX + "OPERATION";
  private static final String DATE = "DATE";
  private static final String DATE_SQL = "SELECT TO_DATE('{}') FROM DUAL";
  private static final String TIME = "TIME";
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String TIMESTAMP_SQL = "SELECT TO_TIMESTAMP('{}') FROM DUAL";
  private static final String TIMESTAMP_HEADER = PREFIX + TIMESTAMP;
  private static final String TABLE = PREFIX + "TABLE";
  private static final String INSERT = "INSERT";
  private static final String DELETE = "DELETE";
  private static final String UPDATE = "UPDATE";
  private static final String SELECT_FOR_UPDATE = "SELECT_FOR_UPDATE";
  private static final int INSERT_CODE = 1;
  private static final int DELETE_CODE = 2;
  private static final int UPDATE_CODE = 3;
  private static final int SELECT_FOR_UPDATE_CODE = 25;
  private static final String NULL = "NULL";

  private final OracleCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private final Properties driverProperties = new Properties();
  private final Map<String, Map<String, Integer>> tableSchemas = new HashMap<>();
  private final Map<String, Map<String, String>> dateTimeColumns = new HashMap<>();
  private final Map<String, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();

  private String initialLogMinerProcedure;
  private String produceLogMinerProcedure;

  private ErrorRecordHandler errorRecordHandler;

  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private PreparedStatement selectChanges;
  private PreparedStatement getLatestSCN;
  private CallableStatement startLogMnr;
  private CallableStatement endLogMnr;
  private Statement dateStatement;
  private final ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
  private final SQLListener sqlListener = new SQLListener();

  public OracleCDCSource(HikariPoolConfigBean hikariConf, OracleCDCConfigBean oracleCDCConfigBean) {
    this.configBean = oracleCDCConfigBean;
    this.hikariConfigBean = hikariConf;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(configBean.baseConfigBean.maxBatchSize, maxBatchSize);
    // Sometimes even though the SCN number has been updated, the select won't return the latest changes for a bit,
    // because the view gets materialized only on calling the SELECT - so the executeQuery may not return anything.
    // To avoid missing data in such cases, we return the new SCN only when we actually read data.
    String nextOffset = lastSourceOffset;
    try {
      if (dataSource == null || dataSource.isClosed()) {
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
      }
      if (connection == null || connection.isClosed()) {
        connection = dataSource.getConnection();
        initializeStatements();
      }
      selectChanges.setMaxRows(batchSize);
      BigDecimal endingSCN = getEndingSCN();
      if (!startLogMiner(lastSourceOffset, endingSCN)) {
        return lastSourceOffset;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated Redo SQL statement: " + redoLogEntriesSql);
      }
      String operation;
      try (ResultSet resultSet = selectChanges.executeQuery()) {
        while (resultSet.next()) {
          nextOffset = resultSet.getBigDecimal(1).toPlainString();
          String username = resultSet.getString(2);
          short op = resultSet.getShort(3);
          String timestamp = resultSet.getString(4);
          String redoSQL = resultSet.getString(5);
          String table = resultSet.getString(6);
          LOG.debug(redoSQL);
          sqlListener.reset();
          plsqlLexer lexer = new plsqlLexer(new ANTLRInputStream(redoSQL));
          CommonTokenStream tokenStream = new CommonTokenStream(lexer);
          plsqlParser parser = new plsqlParser(tokenStream);
          ParserRuleContext ruleContext;
          switch (op) {
            case UPDATE_CODE:
              ruleContext = parser.update_statement();
              operation = UPDATE;
              break;
            case INSERT_CODE:
              ruleContext = parser.insert_statement();
              operation = INSERT;
              break;
            case DELETE_CODE:
              ruleContext = parser.delete_statement();
              operation = DELETE;
              break;
            case SELECT_FOR_UPDATE_CODE:
              ruleContext = parser.update_statement();
              operation = SELECT_FOR_UPDATE;
              break;
            default:
              errorRecordHandler.onError(JDBC_43, redoSQL);
              continue;
          }
          // Walk it and attach our sqlListener
          parseTreeWalker.walk(sqlListener, ruleContext);
          Map<String, String> columns = sqlListener.getColumns();
          Map<String, Field> fields = new HashMap<>();

          Record record = getContext().createRecord(nextOffset);
          Record.Header recordHeader = record.getHeader();

          for (Map.Entry<String, String> column : columns.entrySet()) {
            String columnName = column.getKey();
            fields.put(columnName, objectToField(table, columnName, column.getValue()));
            if (decimalColumns.containsKey(table) && decimalColumns.get(table).containsKey(columnName)) {
              int precision = decimalColumns.get(table).get(columnName).precision;
              int scale = decimalColumns.get(table).get(columnName).scale;
              recordHeader.setAttribute("jdbc." + columnName + ".precision", String.valueOf(precision));
              recordHeader.setAttribute("jdbc." + columnName + ".scale", String.valueOf(scale));
            }
          }
          recordHeader.setAttribute(SCN, nextOffset);
          recordHeader.setAttribute(USER, username);
          recordHeader.setAttribute(OPERATION, operation);
          recordHeader.setAttribute(TIMESTAMP_HEADER, timestamp);
          recordHeader.setAttribute(TABLE, table);
          record.set(Field.create(fields));
          batchMaker.addRecord(record);
        }
      }
      endLogMnr.execute();
      connection.commit();
    } catch (Exception ex) {
      errorRecordHandler.onError(JDBC_44, Throwables.getStackTraceAsString(ex));
    }
    return nextOffset;
  }

  private boolean startLogMiner(String lastSourceOffset, BigDecimal endingSCN) throws SQLException {
    String startString;
    String endingSCNStr = endingSCN.toPlainString();
    if (StringUtils.isEmpty(lastSourceOffset)) {
      startString = getInitialString();
      String logMnrProc = Utils.format(initialLogMinerProcedure, startString);
      LOG.debug("Starting LogMiner using the command: {}", logMnrProc);
      CallableStatement s = connection.prepareCall(logMnrProc);
      s.setBigDecimal(1, endingSCN);
      s.execute();
      s.close();
      connection.commit();
    } else {
      if (lastSourceOffset.equals(endingSCNStr)) {
        return false;
      }
      if (startLogMnr == null) {
        startLogMnr = connection.prepareCall(produceLogMinerProcedure);
      }
      BigDecimal startingSCN = new BigDecimal(lastSourceOffset);
      startLogMnr.setBigDecimal(1, startingSCN.add(BigDecimal.ONE));
      startLogMnr.setBigDecimal(2, endingSCN);
      startLogMnr.execute();
      connection.commit();
    }
    return true;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (!hikariConfigBean.driverClassName.isEmpty()) {
      try {
        Class.forName(hikariConfigBean.driverClassName);
      } catch (ClassNotFoundException e) {
        LOG.error("Hikari Driver class not found.", e);
        issues.add(getContext().createConfigIssue(
            Groups.LEGACY.name(), DRIVER_CLASSNAME, JdbcErrors.JDBC_28, e.toString()));
      }
    }
    issues = hikariConfigBean.validateConfigs(getContext(), issues);
    driverProperties.putAll(hikariConfigBean.driverProperties);
    if (connection == null) { // For tests, we set a mock connection
      try {
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
      } catch (StageException | SQLException e) {
        LOG.error("Error while connecting to DB", e);
        issues.add(getContext().createConfigIssue(
            Groups.CDC.getLabel(), "configBean.database", JDBC_00, configBean.baseConfigBean.database));
        return issues;
      }
    }

    String container = configBean.pdb;
    configBean.baseConfigBean.database =
        configBean.baseConfigBean.caseSensitive ?
            configBean.baseConfigBean.database.trim() :
            configBean.baseConfigBean.database.trim().toUpperCase();
    List<String> tables;
    try(Statement reusedStatement = connection.createStatement()) {
      int majorVersion = getDBVersion(issues);
      // If version is 12+, then the check for table presence must be done in an alternate container!
      if (majorVersion == -1) {
        return issues;
      }
      if (majorVersion >= 12) {
        if (StringUtils.isEmpty(container)) {
          issues.add(getContext().createConfigIssue(Groups.JDBC.getLabel(), "pdb", JDBC_45));
          return issues;
        }
        String switchToPdb = "ALTER SESSION SET CONTAINER = " + configBean.pdb;
        reusedStatement.execute(switchToPdb);
      }
      tables = new ArrayList<>(configBean.baseConfigBean.tables);
      for (String table : configBean.baseConfigBean.tables) {
        table = table.trim();
        if (!configBean.baseConfigBean.caseSensitive) {
          tables.add(table.toUpperCase());
        }
      }
      validateTablePresence(reusedStatement, tables, issues);
      for (String table : tables) {
        table = table.trim();
        tableSchemas.put(table, getTableSchema(table));
      }
      container = CDB_ROOT;
      if (majorVersion >= 12) {
        reusedStatement.execute(SWITCH_TO_CDB_ROOT);
      }
    } catch (SQLException ex) {
      LOG.error("Error while switching to container: " + container, ex);
      issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.getLabel(), USERNAME, JDBC_40, container));
      return issues;
    }

    final String ddlTracking =
        configBean.dictionary.equals(DictionaryValues.DICT_FROM_REDO_LOGS) ? " + DBMS_LOGMNR.DDL_DICT_TRACKING" : "";

    this.initialLogMinerProcedure =  "BEGIN"
        + " DBMS_LOGMNR.START_LOGMNR("
        + " {},"
        + " ENDSCN => ?,"
        + " OPTIONS => DBMS_LOGMNR." + configBean.dictionary.name()
        + "          + DBMS_LOGMNR.CONTINUOUS_MINE"
        + "          + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
        + "          + DBMS_LOGMNR.PRINT_PRETTY_SQL"
        + "          + DBMS_LOGMNR.NO_ROWID_IN_STMT"
        + "          + DBMS_LOGMNR.NO_SQL_DELIMITER"
        + ddlTracking
        + ");"
        + " END;";

    this.produceLogMinerProcedure =  "BEGIN"
        + " DBMS_LOGMNR.START_LOGMNR("
        + " STARTSCN => ?,"
        + " ENDSCN => ?,"
        + " OPTIONS => DBMS_LOGMNR." + configBean.dictionary.name()
        + "          + DBMS_LOGMNR.CONTINUOUS_MINE"
        + "          + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
        + "          + DBMS_LOGMNR.PRINT_PRETTY_SQL"
        + "          + DBMS_LOGMNR.NO_ROWID_IN_STMT"
        + "          + DBMS_LOGMNR.NO_SQL_DELIMITER"
        + ddlTracking
        + ");"
        + " END;";

    redoLogEntriesSql = "SELECT SCN, USERNAME, OPERATION_CODE, TIMESTAMP, SQL_REDO, TABLE_NAME"
        + " FROM V$LOGMNR_CONTENTS"
        + " WHERE OPERATION_CODE IN (1, 2, 3, 25)"
        + " AND SEG_OWNER = "
        + QUOTE
        + configBean.baseConfigBean.database
        + QUOTE
        + " AND TABLE_NAME"
        + " IN "
        + formatTableList(tables)
        + " ORDER BY SCN ASC";

    try {
      initializeStatements();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(getContext().createConfigIssue(
          Groups.CDC.getLabel(), "configBean.database", JDBC_00, configBean.baseConfigBean.database));
    }
    return issues;
  }

  private void initializeStatements() throws SQLException {
    selectChanges = connection.prepareStatement(redoLogEntriesSql);
    getLatestSCN = connection.prepareStatement(CURRENT_SCN);
    endLogMnr = connection.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    dateStatement = connection.createStatement();
  }

  private String formatTableList(List<String> tables) {
    StringBuilder formattedTablesBuilder = new StringBuilder();
    formattedTablesBuilder.append("(");
    Iterator<String> tableIter = tables.iterator();
    while (tableIter.hasNext()) {
      formattedTablesBuilder.append(QUOTE).append(tableIter.next().trim()).append(QUOTE);
      if (tableIter.hasNext()) {
        formattedTablesBuilder.append(COMMA);
      }
    }
    formattedTablesBuilder.append(")");
    return formattedTablesBuilder.toString();
  }

  private String getInitialString() throws SQLException {
    switch(configBean.startValue) {
      case DATE:
        try (Statement s = connection.createStatement()) {
          s.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YYYY HH24:MI:SS'");
        }
        return "STARTTIME => " + QUOTE + configBean.startDate + QUOTE;
      case SCN:
        return START_SCN_OPT_STR + configBean.startSCN;
      case LATEST:
        return START_SCN_OPT_STR + getEndingSCN().toPlainString();
      default:
        throw new IllegalStateException("Unknown Initial Value type: " + configBean.startValue);
    }
  }

  private BigDecimal getEndingSCN() {
    BigDecimal scn = null;
    try (ResultSet rs = getLatestSCN.executeQuery()) {
      if (rs.next()) {
        scn = rs.getBigDecimal(1);
        LOG.info("Current latest SCN is: " + scn.toPlainString());
      }
    } catch (SQLException ex) {
      LOG.error("Error while getting initial SCN", ex);
    }
    return scn;
  }

  private void validateTablePresence(Statement statement, List<String> tables, List<ConfigIssue> issues) {
    for (String table : tables) {
      try {
        statement.execute("SELECT * FROM " + configBean.baseConfigBean.database + "." + table + " WHERE 1 = 0");
      } catch (SQLException ex) {
        LOG.error("Table: " + table + " does not exist", ex);
        issues.add(getContext().createConfigIssue("CDC", "configBean.tables", JDBC_16, table));
      }
    }
  }

  private Map<String, Integer> getTableSchema(String tableName) throws SQLException {
    Map<String, Integer> columns = new HashMap<>();
    String query = "SELECT * FROM " + configBean.baseConfigBean.database + "." + tableName + " WHERE 1 = 0";
    try(Statement schemaStatement = connection.createStatement();
        ResultSet rs = schemaStatement.executeQuery(query)) {
      ResultSetMetaData md = rs.getMetaData();
      int colCount = md.getColumnCount();
      for (int i = 1; i <= colCount; i++) {
        int colType = md.getColumnType(i);
        String colName = md.getColumnName(i);
        if (colType == Types.DATE || colType == Types.TIME || colType == Types.TIMESTAMP) {
          if (dateTimeColumns.get(tableName) == null) {
            dateTimeColumns.put(tableName, new HashMap<String, String>());
          }
          dateTimeColumns.get(tableName).put(colName, md.getColumnTypeName(i));
        }

        if (colType == Types.DECIMAL) {
          if (decimalColumns.get(tableName) == null) {
            decimalColumns.put(tableName, new HashMap<String, PrecisionAndScale>());
          }
          decimalColumns.get(tableName).put(colName, new PrecisionAndScale(md.getPrecision(i), md.getScale(i)));
        }
        columns.put(md.getColumnName(i), md.getColumnType(i));
      }
    }
    return columns;
  }

  private int getDBVersion(List<ConfigIssue> issues) {
    // Getting metadata version using connection.getMetaData().getDatabaseProductVersion() returns 12c which makes
    // comparisons brittle, so use the actual numerical versions.
    try(Statement statement = connection.createStatement();
        ResultSet versionSet = statement.executeQuery("SELECT version FROM product_component_version")) {
      if (versionSet.next()) {
        String versionStr = versionSet.getString("version");
        if (versionStr != null) {
          int majorVersion = Integer.parseInt(versionStr.substring(0, versionStr.indexOf('.')));
          LOG.info("Oracle Version is " + majorVersion);
          return majorVersion;
        }
      }
    } catch (SQLException ex) {
      LOG.error("Error while getting db version info", ex);
      issues.add(getContext().createConfigIssue("JDBC", "configBean.dbHost", JDBC_41));
    }
    return -1;
  }

  @Override
  public void destroy() {
    // Close all statements
    closeStatements(dateStatement, endLogMnr, startLogMnr, selectChanges, getLatestSCN);

    // Connection if it exists
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException ex) {
      LOG.warn("Error while closing connection to database", ex);
    }

    // And finally the hiraki data source
    if(dataSource != null) {
      dataSource.close();
    }
  }

  private void closeStatements(Statement ...statements) {
    if(statements == null) {
      return;
    }

    for(Statement stmt : statements) {
      try {
        if(stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        LOG.warn("Error while closing connection to database", e);
      }
    }
  }

  private Field objectToField(String table, String column, String columnValue) throws SQLException {
    int columnType = tableSchemas.get(table).get(column);
    Field field;
    Field.Type type;
    // All types as of JDBC 2.0 are here:
    // https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
    // Good source of recommended mappings is here:
    // http://www.cs.mun.ca/java-api-1.5/guide/jdbc/getstart/mapping.html
    columnValue = NULL.equalsIgnoreCase(columnValue) ? null : columnValue;
    switch (columnType) {
      case Types.BIGINT:
        field = Field.create(Field.Type.LONG, columnValue);
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        field = Field.create(Field.Type.BYTE_ARRAY, columnValue);
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
      /*
       * Weird behavior seen:
       * If the field is date/time/timestamp the fieldtype returns 93 (timestamp), so we need to check the string
       * representation also.
       * Date/Time/Timestamp.valueOf methods don't always work either - so need to use the SQL TO_DATE method. It is
       * expensive, but no other option :(
       */
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        String colType = dateTimeColumns.get(table).get(column);
        if (DATE.equalsIgnoreCase(colType)) {
          type = Field.Type.DATE;
        } else if (TIME.equalsIgnoreCase(colType)) {
          type = Field.Type.TIME;
        } else if (TIMESTAMP.equalsIgnoreCase(colType)) {
          type = Field.Type.DATETIME;
        } else {
          return null;
        }
        if (columnValue == null) {
          field = Field.create(type, null);
        } else {
          field = getDateTimeField(type, columnValue);
        }
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
        //case Types.REF_CURSOR: // JDK8 only
      case Types.SQLXML:
      case Types.STRUCT:
        //case Types.TIME_WITH_TIMEZONE: // JDK8 only
        //case Types.TIMESTAMP_WITH_TIMEZONE: // JDK8 only
      default:
        return null;
    }

    return field;
  }

  private Field getDateTimeField(Field.Type type, Object value) throws SQLException {
    String sql = Utils.format(type == Field.Type.DATE ? DATE_SQL : TIMESTAMP_SQL, value);
    // This is not the most efficient way, but this works
    // Using valueOf does not work because JDBC returns TIMESTAMP_HEADER as type even when the type is DATE :|
    try (ResultSet rs = dateStatement.executeQuery(sql)) {
      if (rs.next()) {
        if (type == Field.Type.DATE) {
          return Field.create(type, rs.getDate(1));
        } else {
          return Field.create(type, rs.getTimestamp(1));
        }
      } else {
        return null;
      }
    }
  }

  @VisibleForTesting
  void setConnection(Connection conn) {
    this.connection = conn;
  }

  @VisibleForTesting
  void setDataSource(HikariDataSource dataSource) {
    this.dataSource = dataSource;
  }

  private class PrecisionAndScale {
    int precision;
    int scale;
    PrecisionAndScale(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }
  }
}
